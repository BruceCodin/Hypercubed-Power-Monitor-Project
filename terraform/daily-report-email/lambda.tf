# lambda.tf - Lambda Function and IAM roles for daily report email generation

# Data source to get existing ECR repository
data "aws_ecr_repository" "summaries_repo" {
  name = var.ecr_repository_name
}

# Data source to get existing Secrets Manager secret
data "aws_secretsmanager_secret" "db_credentials" {
  arn = var.db_credentials_secret_arn
}

# ==============================================================================
# IAM Role and Policies
# ==============================================================================

resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-daily-report-lambda-role-${var.environment}"
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

  tags = merge(var.common_tags, {
    Name = "${var.project_name}-daily-report-lambda-role"
  })
}

# CloudWatch Logs Policy
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
  role       = aws_iam_role.lambda_execution_role.name
}

# S3 Read Policy for summaries bucket
resource "aws_iam_role_policy" "lambda_s3_read_policy" {
  name = "${var.project_name}-lambda-s3-read-policy-${var.environment}"
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
          "arn:aws:s3:::${var.summaries_bucket_name}",
          "arn:aws:s3:::${var.summaries_bucket_name}/*"
        ]
      }
    ]
  })
}

# ECR Pull Policy
resource "aws_iam_role_policy" "lambda_ecr_policy" {
  name = "${var.project_name}-lambda-ecr-policy-${var.environment}"
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
        Resource = data.aws_ecr_repository.summaries_repo.arn
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
  name = "${var.project_name}-lambda-secrets-policy-${var.environment}"
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
          data.aws_secretsmanager_secret.db_credentials.arn
        ]
      }
    ]
  })
}

# SES Send Email Policy
resource "aws_iam_role_policy" "lambda_ses_policy" {
  name = "${var.project_name}-lambda-ses-policy-${var.environment}"
  role = aws_iam_role.lambda_execution_role.id

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

# ==============================================================================
# Lambda Function Resource
# ==============================================================================

resource "aws_lambda_function" "daily_report_email" {
  function_name = var.lambda_function_name
  role          = aws_iam_role.lambda_execution_role.arn

  package_type = "Image"
  image_uri    = "${data.aws_ecr_repository.summaries_repo.repository_url}:latest"

  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory_size

  environment {
    variables = {
      BUCKET_NAME               = var.summaries_bucket_name
      BUCKET_KEY                = var.summaries_bucket_key
      SENDER_EMAIL              = var.sender_email
      RECIPIENT_EMAILS          = jsonencode(var.recipient_emails)
      EMAIL_SUBJECT             = var.email_subject
      DB_CREDENTIALS_SECRET_ARN = var.db_credentials_secret_arn
    }
  }

  tags = merge(var.common_tags, {
    Name = var.lambda_function_name
  })

  depends_on = [
    aws_iam_role_policy.lambda_ecr_policy,
    aws_iam_role_policy.lambda_s3_read_policy,
    aws_iam_role_policy.lambda_secrets_policy,
    aws_iam_role_policy.lambda_ses_policy
  ]
}

# ==============================================================================
# CloudWatch Log Group
# ==============================================================================

resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = var.lambda_log_retention_days

  tags = merge(var.common_tags, {
    Name = "${var.lambda_function_name}-logs"
  })
}

# CloudWatch Log Group for Step Function
resource "aws_cloudwatch_log_group" "email_step_function_logs" {
  name              = "/aws/stepfunctions/${var.step_function_name}"
  retention_in_days = var.lambda_log_retention_days

  tags = merge(var.common_tags, {
    Name = "${var.step_function_name}-logs"
  })
}

# ==============================================================================
# Lambda Permissions
# ==============================================================================

resource "aws_lambda_permission" "allow_step_functions" {
  statement_id  = "AllowStepFunctionsExecution"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_report_email.function_name
  principal     = "states.amazonaws.com"
  source_arn    = "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:*"
}