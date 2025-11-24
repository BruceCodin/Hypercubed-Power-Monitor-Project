# lambda.tf - Lambda Function for AI Summary Generation


# Data source to get existing ECR repository
data "aws_ecr_repository" "ai_summary_repo" {
  name = var.ecr_repository_name
}

# IAM Role and Policies for Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name = "power-monitor-ai-summary-lambda-role"

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
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# CloudWatch Logs Policy
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}

# ECR Pull Policy
resource "aws_iam_role_policy" "lambda_ecr_policy" {
  name = "lambda-ecr-access-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:BatchCheckLayerAvailability"
        ]
        Resource = data.aws_ecr_repository.ai_summary_repo.arn
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      }
    ]
  })
}

# Secrets Manager Policy
resource "aws_iam_role_policy" "lambda_secrets_policy" {
  name = "lambda-secrets-access-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = [
          var.db_credentials_secret_arn,
          aws_secretsmanager_secret.openai_key.arn
        ]
      }
    ]
  })
}

# S3 Read Policy
resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "lambda-s3-read-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.historical_data_bucket_name}",
          "arn:aws:s3:::${var.historical_data_bucket_name}/*"
        ]
      }
    ]
  })
}

# Lambda Function Resource
resource "aws_lambda_function" "ai_summary_generator" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda_execution_role.arn
  
  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.ai_summary_repo.repository_url}:latest"
  
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  environment {
    variables = {
      DB_CREDENTIALS_SECRET_ARN = var.db_credentials_secret_arn
      OPENAI_SECRET_ARN         = aws_secretsmanager_secret.openai_key.arn
      S3_BUCKET_NAME            = var.historical_data_bucket_name
      AWS_REGION                = var.aws_region
    }
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }

  depends_on = [
    aws_iam_role_policy.lambda_ecr_policy,
    aws_iam_role_policy.lambda_secrets_policy,
    aws_iam_role_policy.lambda_s3_policy
  ]
}


# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = 7

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}


# Outputs
output "lambda_function_name" {
  description = "Name of the AI summary Lambda function"
  value       = aws_lambda_function.ai_summary_generator.function_name
}

output "lambda_function_arn" {
  description = "ARN of the AI summary Lambda function"
  value       = aws_lambda_function.ai_summary_generator.arn
}

output "lambda_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = aws_iam_role.lambda_execution_role.arn
}