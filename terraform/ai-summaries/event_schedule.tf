# eventbridge.tf - EventBridge Scheduler for Daily AI Summaries

# IAM Role for EventBridge Scheduler
resource "aws_iam_role" "eventbridge_scheduler_role" {
  name = "power-monitor-ai-summary-scheduler-role"

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

# Policy to allow EventBridge to invoke Lambda
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
        Resource = aws_lambda_function.ai_summary_generator.arn
      }
    ]
  })
}


# EventBridge Scheduler Resource
resource "aws_scheduler_schedule" "ai_summary_schedule" {
  name       = "power-monitor-ai-summary-daily"
  group_name = "default"

  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = var.schedule_expression

  target {
    arn      = aws_lambda_function.ai_summary_generator.arn
    role_arn = aws_iam_role.eventbridge_scheduler_role.arn
    
    input = jsonencode({
      prompt = "Generate a comprehensive daily summary of UK energy data including power generation, carbon intensity, pricing, and power outages. Compare today's metrics with historical trends."
    })
  }

  state = var.schedule_enabled ? "ENABLED" : "DISABLED"
}

# ==============================================================================
# Outputs
# ==============================================================================

output "eventbridge_schedule_name" {
  description = "Name of the EventBridge schedule"
  value       = aws_scheduler_schedule.ai_summary_schedule.name
}

output "eventbridge_schedule_arn" {
  description = "ARN of the EventBridge schedule"
  value       = aws_scheduler_schedule.ai_summary_schedule.arn
}

output "eventbridge_schedule_state" {
  description = "Current state of the EventBridge schedule (ENABLED/DISABLED)"
  value       = aws_scheduler_schedule.ai_summary_schedule.state
}