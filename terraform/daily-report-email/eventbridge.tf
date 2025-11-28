# EventBridge Rule - Triggers Email Step Function according to schedule

resource "aws_cloudwatch_event_rule" "email_pipeline_schedule" {
  name                = "${var.project_name}-email-pipeline-schedule-${var.environment}"
  description         = "Trigger email notification pipeline Step Function"
  schedule_expression = var.schedule_expression
  state               = var.schedule_enabled ? "ENABLED" : "DISABLED"

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-email-pipeline-schedule"
  })
}

# EventBridge Target - Points to Email Step Function
resource "aws_cloudwatch_event_target" "email_step_function_target" {
  rule      = aws_cloudwatch_event_rule.email_pipeline_schedule.name
  target_id = "EmailPipelineStepFunctionTarget"
  arn       = aws_sfn_state_machine.email_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_step_function_role.arn
}
