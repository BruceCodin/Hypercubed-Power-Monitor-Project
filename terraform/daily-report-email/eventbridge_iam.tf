# IAM Role for EventBridge to invoke Step Function
resource "aws_iam_role" "eventbridge_step_function_role" {
  name = "${var.project_name}-eventbridge-step-function-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-eventbridge-step-function-role"
  })
}

# IAM Policy for EventBridge to start Step Function execution
resource "aws_iam_role_policy" "eventbridge_step_function_policy" {
  name = "${var.project_name}-eventbridge-step-function-execution-policy-${var.environment}"
  role = aws_iam_role.eventbridge_step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = aws_sfn_state_machine.email_pipeline.arn
      }
    ]
  })
}
