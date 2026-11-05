output "api_url" {
  value = aws_apigatewayv2_stage.default.invoke_url
}

output "table_name" {
  value = aws_dynamodb_table.tracks.name
}

output "function_name" {
  value = aws_lambda_function.peer.function_name
}

output "log_group" {
  value = aws_cloudwatch_log_group.peer.name
}
