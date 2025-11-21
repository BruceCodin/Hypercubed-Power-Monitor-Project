# IAM Role for EventBridge Scheduler
resource "aws_iam_role" "eventbridge_scheduler_role" {
  name = "power-monitor-eventbridge-scheduler-role"

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

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# Policy to allow EventBridge Scheduler to invoke Lambda
resource "aws_iam_role_policy" "eventbridge_invoke_lambda" {
  name = "eventbridge-invoke-lambda-policy"
  role = aws_iam_role.eventbridge_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.power_cuts_etl.arn
      }
    ]
  })
}

# EventBridge Scheduler - Runs every 5 minutes
resource "aws_scheduler_schedule" "power_cuts_schedule" {
  name       = "power-cuts-etl-schedule"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  # Run every 5 minutes
  schedule_expression = "rate(5 minutes)"

  target {
    arn      = aws_lambda_function.power_cuts_etl.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
  }

  state = var.scheduler_enabled ? "ENABLED" : "DISABLED"
}

# Outputs
output "eventbridge_schedule_name" {
  description = "Name of the EventBridge schedule"
  value       = aws_scheduler_schedule.power_cuts_schedule.name
}

output "eventbridge_schedule_arn" {
  description = "ARN of the EventBridge schedule"
  value       = aws_scheduler_schedule.power_cuts_schedule.arn
}

output "eventbridge_schedule_state" {
  description = "Current state of the EventBridge schedule"
  value       = aws_scheduler_schedule.power_cuts_schedule.state
}