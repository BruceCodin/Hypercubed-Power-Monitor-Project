# IAM role for Lambda execution
resource "aws_iam_role" "neso_lambda_execution_role" {
  name = "neso-etl-lambda-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "neso-etl-lambda-execution-role"
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# Attach basic Lambda execution policy
resource "aws_iam_role_policy_attachment" "neso_lambda_basic_execution" {
  role       = aws_iam_role.neso_lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Attach VPC execution policy (needed for Lambda to access RDS in VPC)
resource "aws_iam_role_policy_attachment" "neso_lambda_vpc_execution" {
  role       = aws_iam_role.neso_lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Policy to access Secrets Manager
resource "aws_iam_role_policy" "neso_lambda_secrets_policy" {
  name = "neso-lambda-secrets-policy"
  role = aws_iam_role.neso_lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = data.aws_secretsmanager_secret.db_credentials.arn
      }
    ]
  })
}

# Reference existing Secrets Manager secret
data "aws_secretsmanager_secret" "db_credentials" {
  name = var.db_secret_name
}

# Lambda function
resource "aws_lambda_function" "neso_etl_lambda" {
  function_name = "neso-etl-lambda"
  role          = aws_iam_role.neso_lambda_execution_role.arn

  package_type = "Image"
  image_uri    = "${aws_ecr_repository.power_monitor_repo.repository_url}:latest"

  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory

  # Environment variables
  environment {
    variables = {
      DB_SECRET_ARN = data.aws_secretsmanager_secret.db_credentials.arn
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.neso_lambda_basic_execution,
    aws_iam_role_policy.neso_lambda_secrets_policy
  ]

  tags = {
    Name        = "neso-etl-lambda"
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# CloudWatch log group for Lambda
resource "aws_cloudwatch_log_group" "neso_lambda_log_group" {
  name              = "/aws/lambda/neso-etl-lambda"
  retention_in_days = var.log_retention_days

  tags = {
    Name        = "neso-etl-lambda-logs"
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# EventBridge rule to trigger Lambda once a day (at 2 AM UTC)
resource "aws_cloudwatch_event_rule" "neso_lambda_schedule" {
  name                = "neso-etl-daily-schedule"
  description         = "Trigger NESO ETL Lambda function once a day at 2 AM UTC"
  schedule_expression = "cron(0 10 * * ? *)"

  tags = {
    Name        = "neso-etl-schedule"
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# EventBridge target
resource "aws_cloudwatch_event_target" "neso_lambda_target" {
  rule      = aws_cloudwatch_event_rule.neso_lambda_schedule.name
  target_id = "neso-etl-lambda"
  arn       = aws_lambda_function.neso_etl_lambda.arn
}

# Permission for EventBridge to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.neso_etl_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.neso_lambda_schedule.arn
}

# Outputs
output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.neso_etl_lambda.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.neso_etl_lambda.arn
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.neso_lambda_schedule.name
}
