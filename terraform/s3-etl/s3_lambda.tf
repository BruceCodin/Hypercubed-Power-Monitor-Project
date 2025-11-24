# Data source to reference existing Secrets Manager secret
data "aws_secretsmanager_secret" "existing_credentials" {
  arn = var.secrets_manager_arn
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name = "power-monitor-lambda-execution-role"

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

# Attach basic Lambda execution policy (for CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}

# Policy to allow Lambda to pull images from ECR
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
        Resource = aws_ecr_repository.power_monitor_repo.arn
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

# Policy to allow Lambda to access Secrets Manager
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
        Resource = var.secrets_manager_arn
      }
    ]
  })
}

# Policy to allow Lambda to write to S3
resource "aws_iam_role_policy" "lambda_s3_policy" {
  name = "lambda-s3-write-policy"
  role = aws_iam_role.lambda_execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Lambda Function
resource "aws_lambda_function" "power_cuts_etl" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda_execution_role.arn
  
  # Image from ECR
  package_type = "Image"
  image_uri    = "${aws_ecr_repository.power_monitor_repo.repository_url}:latest"
  
  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  # Pass secret ARN as environment variable (not the actual secrets!)
  environment {
    variables = {
      SECRETS_MANAGER_ARN = var.secrets_manager_arn
    }
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }

  # Prevents Terraform from trying to deploy before image exists
  depends_on = [aws_ecr_repository.power_monitor_repo]
}

# CloudWatch Log Group for Lambda
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
  description = "Name of the Lambda function"
  value       = aws_lambda_function.power_cuts_etl.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.power_cuts_etl.arn
}