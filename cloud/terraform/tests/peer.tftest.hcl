mock_provider "aws" {}

# Computed ARNs are unknown at plan time, so pin them for the policy checks.
override_resource {
  target          = aws_dynamodb_table.tracks
  override_during = plan
  values = {
    arn = "arn:aws:dynamodb:us-east-1:123456789012:table/holdfast-tracks"
  }
}

override_resource {
  target          = aws_cloudwatch_log_group.peer
  override_during = plan
  values = {
    arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/lambda/holdfast-peer"
  }
}

variables {
  peer_token     = "0123456789abcdef0123456789abcdef"
  alert_email    = "me@example.com"
  bootstrap_path = "tests/fixtures/bootstrap"
}

run "function_is_small_arm_and_unreserved" {
  command = plan

  assert {
    condition     = aws_lambda_function.peer.runtime == "provided.al2023"
    error_message = "runtime must be provided.al2023"
  }
  assert {
    condition     = length(aws_lambda_function.peer.architectures) == 1 && contains(aws_lambda_function.peer.architectures, "arm64")
    error_message = "architectures must be exactly arm64"
  }
  assert {
    condition     = aws_lambda_function.peer.reserved_concurrent_executions == null || aws_lambda_function.peer.reserved_concurrent_executions == -1
    error_message = "no reserved concurrency"
  }
  assert {
    condition     = aws_lambda_function.peer.memory_size == 128 && aws_lambda_function.peer.timeout == 10
    error_message = "128 MB and 10 s"
  }
}

run "table_is_on_demand_with_ttl" {
  command = plan

  assert {
    condition     = aws_dynamodb_table.tracks.billing_mode == "PAY_PER_REQUEST"
    error_message = "table must be on demand"
  }
  assert {
    condition     = aws_dynamodb_table.tracks.hash_key == "icao" && aws_dynamodb_table.tracks.range_key == null
    error_message = "hash key icao only"
  }
  assert {
    condition     = one(aws_dynamodb_table.tracks.ttl).attribute_name == "expires" && one(aws_dynamodb_table.tracks.ttl).enabled
    error_message = "TTL on expires"
  }
}

run "iam_policy_is_exactly_least_privilege" {
  command = plan

  assert {
    condition     = length(jsondecode(aws_iam_role_policy.peer.policy).Statement) == 2
    error_message = "exactly two statements"
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.peer.policy).Statement[0].Action == ["dynamodb:Scan", "dynamodb:PutItem"] && jsondecode(aws_iam_role_policy.peer.policy).Statement[0].Resource == aws_dynamodb_table.tracks.arn
    error_message = "dynamodb statement must be Scan and PutItem on the table only"
  }
  assert {
    condition     = jsondecode(aws_iam_role_policy.peer.policy).Statement[1].Action == ["logs:CreateLogStream", "logs:PutLogEvents"] && jsondecode(aws_iam_role_policy.peer.policy).Statement[1].Resource == "${aws_cloudwatch_log_group.peer.arn}:*"
    error_message = "logs statement must be two actions on the log group only"
  }
  assert {
    condition     = alltrue([for s in jsondecode(aws_iam_role_policy.peer.policy).Statement : s.Effect == "Allow"])
    error_message = "allow statements only"
  }
  assert {
    condition     = length(jsondecode(aws_iam_role.peer.assume_role_policy).Statement) == 1 && jsondecode(aws_iam_role.peer.assume_role_policy).Statement[0].Principal.Service == "lambda.amazonaws.com"
    error_message = "only the Lambda service may assume the role"
  }
}

run "logs_expire_after_a_week" {
  command = plan

  assert {
    condition     = aws_cloudwatch_log_group.peer.retention_in_days == 7
    error_message = "retention must be 7 days"
  }
}

run "stage_throttling_uses_variables" {
  command = plan

  variables {
    burst = 20
    rate  = 8
  }

  assert {
    condition     = one(aws_apigatewayv2_stage.default.default_route_settings).throttling_burst_limit == 20 && one(aws_apigatewayv2_stage.default.default_route_settings).throttling_rate_limit == 8
    error_message = "throttling must follow burst and rate"
  }
}

run "short_peer_token_is_rejected" {
  command = plan

  variables {
    peer_token = "too-short"
  }

  expect_failures = [var.peer_token]
}
