# AWS Provider Configuration
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

# ECR Repository
variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "c20-power-monitor-powercuts-etl-repo"
}

# Lambda Configuration
variable "lambda_function_name" {
  description = "Name of the Lambda function"
  type        = string
  default     = "c20-power-monitor-powercuts-etl-lambda"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300  # 5 minutes
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 512
}

# Secrets Manager
variable "secrets_manager_arn" {
  description = "ARN of the existing Secrets Manager secret containing DB credentials and API keys"
  type        = string
  default     = "arn:aws:secretsmanager:eu-west-2:129033205317:secret:c20-power-monitor-db-credentials-TAc5Xx"
}