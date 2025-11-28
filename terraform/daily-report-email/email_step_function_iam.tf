# IAM Role and Policies for Email Step Function


# IAM Role for Email Step Function
resource "aws_iam_role" "email_step_function_role" {
  name = "${var.project_name}-email-step-function-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-email-step-function-role"
  })
}


# Policy to allow Step Function to invoke Lambda
resource "aws_iam_role_policy" "email_step_function_lambda_policy" {
  name = "${var.project_name}-email-step-function-lambda-invoke-${var.environment}"
  role = aws_iam_role.email_step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = aws_lambda_function.daily_report_email.arn
      }
    ]
  })
}


# Policy to allow Step Function to send emails via SES
resource "aws_iam_role_policy" "email_step_function_ses_policy" {
  name = "${var.project_name}-email-step-function-ses-send-${var.environment}"
  role = aws_iam_role.email_step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ses:SendEmail",
          "ses:SendRawEmail"
        ]
        Resource = "*"
      }
    ]
  })
}


# Policy to allow Step Function to write logs to CloudWatch
resource "aws_iam_role_policy" "email_step_function_logs_policy" {
  name = "${var.project_name}-email-step-function-cloudwatch-logs-${var.environment}"
  role = aws_iam_role.email_step_function_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutLogEvents",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}


# Data sources for dynamic ARN construction
data "aws_region" "current" {}

data "aws_caller_identity" "current" {}
