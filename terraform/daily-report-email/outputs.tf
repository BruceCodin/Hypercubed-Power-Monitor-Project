# outputs.tf - Outputs for Daily Report Email infrastructure

# ==============================================================================
# Lambda Function Outputs
# ==============================================================================

output "lambda_function_arn" {
  description = "ARN of the daily report email Lambda function"
  value       = aws_lambda_function.daily_report_email.arn
}

output "lambda_function_name" {
  description = "Name of the daily report email Lambda function"
  value       = aws_lambda_function.daily_report_email.function_name
}

output "lambda_function_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_execution_role.arn
}

output "lambda_cloudwatch_log_group" {
  description = "CloudWatch log group name for Lambda function"
  value       = aws_cloudwatch_log_group.lambda_logs.name
}

output "lambda_cloudwatch_log_group_arn" {
  description = "CloudWatch log group ARN for Lambda function"
  value       = aws_cloudwatch_log_group.lambda_logs.arn
}

# ==============================================================================
# Step Functions Outputs
# ==============================================================================

output "stepfunctions_state_machine_arn" {
  description = "ARN of the daily report Step Functions state machine"
  value       = aws_sfn_state_machine.daily_report_workflow.arn
}

output "stepfunctions_state_machine_name" {
  description = "Name of the daily report Step Functions state machine"
  value       = aws_sfn_state_machine.daily_report_workflow.name
}

output "stepfunctions_execution_role_arn" {
  description = "ARN of the Step Functions execution role"
  value       = aws_iam_role.stepfunctions_execution_role.arn
}

# ==============================================================================
# EventBridge Scheduler Outputs
# ==============================================================================

output "eventbridge_schedule_arn" {
  description = "ARN of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.daily_report_trigger.arn
}

output "eventbridge_schedule_name" {
  description = "Name of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.daily_report_trigger.name
}

output "eventbridge_schedule_state" {
  description = "Current state of the EventBridge Scheduler (ENABLED/DISABLED)"
  value       = aws_scheduler_schedule.daily_report_trigger.state
}

# ==============================================================================
# SES Configuration Outputs
# ==============================================================================

output "ses_configuration_set_name" {
  description = "Name of the SES configuration set for bounce/complaint tracking"
  value       = var.ses_enabled ? aws_ses_configuration_set.main[0].name : null
}

output "ses_sender_email" {
  description = "Verified sender email address for SES"
  value       = var.sender_email
}

output "sns_notification_topic_arn" {
  description = "ARN of the SNS topic for SES bounce/complaint notifications"
  value       = var.ses_enabled ? aws_sns_topic.ses_notifications[0].arn : null
}

# ==============================================================================
# Configuration Summary Outputs
# ==============================================================================

output "summary" {
  description = "Summary of deployed infrastructure"
  value = {
    project_name         = var.project_name
    environment          = var.environment
    region               = var.aws_region
    lambda_function_name = aws_lambda_function.daily_report_email.function_name
    state_machine_name   = aws_sfn_state_machine.daily_report_workflow.name
    schedule_expression  = var.schedule_expression
    schedule_timezone    = var.schedule_timezone
    schedule_enabled     = var.schedule_enabled
    email_subject        = var.email_subject
    summaries_bucket     = var.summaries_bucket_name
    sender_email         = var.sender_email
    recipient_count      = length(var.recipient_emails)
  }
}
