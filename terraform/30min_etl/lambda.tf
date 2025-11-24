# IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name = "power-monitor-30min-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

# Attach basic Lambda execution policy
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_role.name
}

# IAM Policy for Secrets Manager access
resource "aws_iam_role_policy" "secrets_manager_policy" {
  name = "secrets-manager-access"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "secretsmanager:GetSecretValue"
      ]
      Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:c20-power-monitor-db-credentials-*"
    }]
  })
}

# Lambda Function
resource "aws_lambda_function" "power_monitor_30min" {
  function_name = "c20-power-monitor-30min-etl"
  role          = aws_iam_role.lambda_role.arn

  # Using Docker image from ECR
  package_type = "Image"
  image_uri    = "${aws_ecr_repository.power_monitor_repo.repository_url}:latest"

  timeout     = 300  # 5 minutes
  memory_size = 512  # MB

  environment {
    variables = {
      ENVIRONMENT = "production"
    }
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
    Purpose     = "30min ETL Lambda for Carbon and Elexon Data"
  }
}

# EventBridge Rule to trigger every 30 minutes
resource "aws_cloudwatch_event_rule" "every_30_minutes" {
  name                = "power-monitor-30min-schedule"
  description         = "Trigger Lambda every 30 minutes"
  schedule_expression = "cron(0/30 * * * ? *)"
}

# EventBridge Target
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.every_30_minutes.name
  target_id = "PowerMonitor30minLambda"
  arn       = aws_lambda_function.power_monitor_30min.arn
}

# Permission for EventBridge to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.power_monitor_30min.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.every_30_minutes.arn
}

# Outputs
output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.power_monitor_30min.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.power_monitor_30min.arn
}
