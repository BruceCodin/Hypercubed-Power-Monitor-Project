# EventBridge Rule to trigger Lambda at 23:55 daily
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "power-monitor-s3-power-gen-etl-daily-trigger"
  description         = "Triggers S3 ETL Lambda function at 23:55 UTC daily"
  schedule_expression = "cron(55 23 * * ? *)"

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# EventBridge Target
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.daily_trigger.name
  target_id = "PowerMonitorS3ETLLambda"
  arn       = aws_lambda_function.power_cuts_etl.arn
}

# Permission for EventBridge to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.power_cuts_etl.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_trigger.arn
}