# stepfunctions.tf - AWS Step Functions state machine for daily report email workflow

# ==============================================================================
# IAM Role for Step Functions
# ==============================================================================

resource "aws_iam_role" "stepfunctions_execution_role" {
  name = "${var.project_name}-stepfunctions-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-stepfunctions-execution-role"
  })
}

# Policy to allow Step Functions to invoke Lambda
resource "aws_iam_role_policy" "stepfunctions_invoke_lambda" {
  name = "${var.project_name}-stepfunctions-invoke-lambda-${var.environment}"
  role = aws_iam_role.stepfunctions_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = [
          aws_lambda_function.daily_report_email.arn,
          "${aws_lambda_function.daily_report_email.arn}:*"
        ]
      }
    ]
  })
}

# ==============================================================================
# Step Functions State Machine Definition
# ==============================================================================

# Define the state machine logic as a local variable for clarity
locals {
  state_machine_definition = {
    Comment = "Daily Report Email Workflow - Generates and sends daily power monitor report"
    StartAt = "GenerateReport"
    States = {
      GenerateReport = {
        Type     = "Task"
        Resource = aws_lambda_function.daily_report_email.arn
        Next     = "SendEmailSuccess"
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            Next        = "SendEmailFailure"
          }
        ]
        TimeoutSeconds = var.lambda_timeout + 60
      }
      SendEmailSuccess = {
        Type    = "Succeed"
        Comment = "Daily report email sent successfully"
      }
      SendEmailFailure = {
        Type  = "Fail"
        Error = "EmailGenerationFailed"
        Cause = "Failed to generate and send daily report email"
      }
    }
  }
}

resource "aws_sfn_state_machine" "daily_report_workflow" {
  name       = var.step_function_name
  role_arn   = aws_iam_role.stepfunctions_execution_role.arn
  definition = jsonencode(local.state_machine_definition)

  tags = merge(var.common_tags, {
    Name = var.step_function_name
  })

  depends_on = [
    aws_iam_role_policy.stepfunctions_invoke_lambda
  ]
}

# ==============================================================================
# EventBridge Scheduler to trigger Step Functions
# ==============================================================================

resource "aws_iam_role" "eventbridge_scheduler_role" {
  name = "${var.project_name}-eventbridge-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-eventbridge-scheduler-role"
  })
}

# Policy to allow EventBridge Scheduler to invoke Step Functions
resource "aws_iam_role_policy" "eventbridge_invoke_stepfunctions" {
  name = "${var.project_name}-eventbridge-invoke-stepfunctions-${var.environment}"
  role = aws_iam_role.eventbridge_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.daily_report_workflow.arn
      }
    ]
  })
}

# EventBridge Scheduler resource
resource "aws_scheduler_schedule" "daily_report_trigger" {
  name       = "${var.project_name}-daily-report-trigger-${var.environment}"
  group_name = "default"

  description = "Triggers daily power monitor report email workflow"

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 15
  }

  schedule_expression          = var.schedule_expression
  schedule_expression_timezone = var.schedule_timezone
  state                        = var.schedule_enabled ? "ENABLED" : "DISABLED"

  target {
    arn      = aws_sfn_state_machine.daily_report_workflow.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn

    # Provide execution name to make each execution unique
    input = jsonencode({
      executionName = "daily-report-${formatdate("YYYY-MM-DD-hhmm", timestamp())}"
    })
  }

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-daily-report-trigger"
  })
}

# ==============================================================================
# Outputs
# ==============================================================================

output "state_machine_arn" {
  description = "ARN of the daily report Step Functions state machine"
  value       = aws_sfn_state_machine.daily_report_workflow.arn
}

output "state_machine_name" {
  description = "Name of the daily report Step Functions state machine"
  value       = aws_sfn_state_machine.daily_report_workflow.name
}

output "scheduler_schedule_arn" {
  description = "ARN of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.daily_report_trigger.arn
}

output "scheduler_schedule_name" {
  description = "Name of the EventBridge Scheduler schedule"
  value       = aws_scheduler_schedule.daily_report_trigger.name
}
