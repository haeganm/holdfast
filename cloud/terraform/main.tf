locals {
  function_name = "${var.name}-peer"
}

#trivy:ignore:AWS-0024 demo data, rebuilt from edge nodes, PITR adds cost
#trivy:ignore:AWS-0025 AWS-owned key is free; a CMK bills monthly while idle
resource "aws_dynamodb_table" "tracks" {
  name         = "${var.name}-tracks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "icao"

  attribute {
    name = "icao"
    type = "S"
  }

  # The Lambda writes epoch seconds here so stale aircraft age out.
  ttl {
    attribute_name = "expires"
    enabled        = true
  }

  # A hard ceiling on the on-demand rate, so a runaway client is throttled
  # instead of billed. Only token holders reach the table at all.
  on_demand_throughput {
    max_read_request_units  = var.max_read_units
    max_write_request_units = var.max_write_units
  }

  # Demo: no point-in-time recovery, and the table can be destroyed freely.
  point_in_time_recovery {
    enabled = false
  }
  deletion_protection_enabled = false
}

# Created explicitly so Lambda does not make one that never expires.
#trivy:ignore:AWS-0017 service-default encryption is enough, a CMK bills while idle
resource "aws_cloudwatch_log_group" "peer" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = 7
}

# The trust policy is the one AWS documents for execution roles, with no
# aws:SourceAccount condition. Lambda only accepts an execution role from the
# function's own account, and attaching one takes iam:PassRole here, so there
# is no confused deputy for the condition to stop; and a condition key Lambda
# didn't send would break CreateFunction.
resource "aws_iam_role" "peer" {
  name = "${local.function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "peer" {
  name = "least-privilege"
  role = aws_iam_role.peer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["dynamodb:Scan", "dynamodb:PutItem"]
        Resource = aws_dynamodb_table.tracks.arn
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "${aws_cloudwatch_log_group.peer.arn}:*"
      },
    ]
  })
}

# output_file_mode keeps the binary executable when the zip is built on Windows.
data "archive_file" "bootstrap" {
  type             = "zip"
  source_file      = var.bootstrap_path
  output_path      = "${path.module}/build/${local.function_name}.zip"
  output_file_mode = "0755"
}

# No reserved or provisioned concurrency: new accounts have a small quota and
# provisioned concurrency bills while idle.
#trivy:ignore:AWS-0066 X-Ray needs extra IAM actions and the demo has no use for traces
resource "aws_lambda_function" "peer" {
  function_name    = local.function_name
  role             = aws_iam_role.peer.arn
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  handler          = "bootstrap"
  memory_size      = 128
  timeout          = 10
  filename         = data.archive_file.bootstrap.output_path
  source_code_hash = data.archive_file.bootstrap.output_base64sha256

  environment {
    variables = {
      TABLE      = aws_dynamodb_table.tracks.name
      PEER_TOKEN = var.peer_token
    }
  }

  depends_on = [aws_cloudwatch_log_group.peer, aws_iam_role_policy.peer]
}

resource "aws_apigatewayv2_api" "peer" {
  name          = local.function_name
  protocol_type = "HTTP"
}

resource "aws_apigatewayv2_integration" "peer" {
  api_id                 = aws_apigatewayv2_api.peer.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.peer.invoke_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "default" {
  api_id    = aws_apigatewayv2_api.peer.id
  route_key = "$default"
  target    = "integrations/${aws_apigatewayv2_integration.peer.id}"
}

# Access logs only cost when requests arrive, and expire like the function logs.
#trivy:ignore:AWS-0017 service-default encryption is enough, a CMK bills while idle
resource "aws_cloudwatch_log_group" "access" {
  name              = "/aws/apigateway/${local.function_name}"
  retention_in_days = 7
}

resource "aws_apigatewayv2_stage" "default" {
  api_id      = aws_apigatewayv2_api.peer.id
  name        = "$default"
  auto_deploy = true

  access_log_settings {
    destination_arn = aws_cloudwatch_log_group.access.arn
    format = jsonencode({
      requestId = "$context.requestId"
      ip        = "$context.identity.sourceIp"
      time      = "$context.requestTime"
      route     = "$context.routeKey"
      status    = "$context.status"
      latency   = "$context.responseLatency"
    })
  }

  default_route_settings {
    throttling_burst_limit = var.burst
    throttling_rate_limit  = var.rate
  }
}

resource "aws_lambda_permission" "apigw" {
  statement_id  = "AllowHttpApiInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.peer.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.peer.execution_arn}/*/*"
}

# A budget only alerts by email. It does not cap or stop spend.
resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  time_unit    = "MONTHLY"
  limit_amount = var.budget_usd
  limit_unit   = "USD"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
