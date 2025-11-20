provider "aws" {
  region     = var.aws_region
}

# === IMPORTANT ===
# Update service_name in variables.tf before using this template
# Update security group and subnet IDs to match RDS
# Currently set up to allow the ETL code to connect to the RDS directly via secrets arn

resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.service_name}-lambda-execution-role"

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
    Name = "${var.service_name}-lambda-execution-role"
  }
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_secrets_policy" {
  name = "${var.service_name}-lambda-secrets-policy"
  role = aws_iam_role.lambda_execution_role.id

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

data "aws_ecr_repository" "existing_repo" {
  name = var.ecr_repository_name
}

# Reference the existing Secrets Manager secret
data "aws_secretsmanager_secret" "db_credentials" {
  name = var.db_secret_name
}

resource "aws_lambda_function" "etl_lambda" {
  function_name = var.service_name
  role          = aws_iam_role.lambda_execution_role.arn

  package_type = "Image"
  image_uri = "${data.aws_ecr_repository.existing_repo.repository_url}:latest"

  timeout     = var.lambda_timeout
  memory_size = var.lambda_memory

  # VPC configuration for RDS connectivity
  vpc_config {
    subnet_ids         = var.lambda_subnet_ids
    security_group_ids = var.lambda_security_group_ids
  }

  # Environment variables - Lambda will need to fetch secrets at runtime
  # The application code should retrieve these from Secrets Manager using the ARN
  environment {
    variables = {
      DB_SECRET_ARN = data.aws_secretsmanager_secret.db_credentials.arn
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic_execution,
    aws_iam_role_policy.lambda_secrets_policy
  ]

  tags = {
    Name = "${var.service_name}-lambda"
  }
}

resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${var.service_name}"
  retention_in_days = var.log_retention_days

  tags = {
    Name = "${var.service_name}-logs"
  }
}

# EventBridge rule to trigger Lambda
#resource "aws_cloudwatch_event_rule" "lambda_schedule" {
#  name                = "${var.service_name}-schedule"
#  description         = "Trigger ETL Lambda function every <choose freq>"
#  schedule_expression = "<choose freq>" #"cron(* * * * ? *)"
#}
#
#resource "aws_cloudwatch_event_target" "lambda_target" {
#  rule      = aws_cloudwatch_event_rule.lambda_schedule.name
#  target_id = "lambda"
#  arn       = aws_lambda_function.etl_lambda.arn
#}
#
#resource "aws_lambda_permission" "allow_eventbridge" {
#  statement_id  = "AllowExecutionFromEventBridge"
#  action        = "lambda:InvokeFunction"
#  function_name = aws_lambda_function.etl_lambda.function_name
#  principal     = "events.amazonaws.com"
#  source_arn    = aws_cloudwatch_event_rule.lambda_schedule.arn
#}