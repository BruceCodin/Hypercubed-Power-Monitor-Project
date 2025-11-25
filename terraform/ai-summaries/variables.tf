# variables.tf - Variables for AI Summary infrastructure
# AWS Configuration
# ==============================================================================

variable "aws_region" {
  description = "AWS region for deploying resources"
  type        = string
  default     = "eu-west-2"
}

# ECR Configuration
# ==============================================================================

variable "ecr_repository_name" {
  description = "Name of the ECR repository for AI summary Lambda Docker image"
  type        = string
  default     = "c20-power-monitor-ai-summary-repo"
}

# Lambda Configuration
# ==============================================================================

variable "lambda_function_name" {
  description = "Name of the AI summary Lambda function"
  type        = string
  default     = "c20-power-monitor-ai-summary-lambda"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds (max 900 for Lambda)"
  type        = number
  default     = 600  # 10 minutes for AI processing
}

variable "lambda_memory_size" {
  description = "Lambda memory allocation in MB (128 MB to 10,240 MB)"
  type        = number
  default     = 1024  # Higher memory for AI/data processing
}

variable "historical_data_bucket_name" { # S3 bucket name for historical data
  description = "Name of S3 bucket containing historical energy data (created by data team)"
  type        = string
}

# Secrets Manager Configuration
# ==============================================================================

variable "openai_api_key" {
  description = "OpenAI API key for generating AI summaries"
  type        = string
  sensitive   = true
}

variable "db_credentials_secret_arn" {
  description = "ARN of existing Secrets Manager secret containing database credentials and API keys"
  type        = string
  sensitive   = true
}

# EventBridge Scheduler Configuration
# ==============================================================================

variable "schedule_enabled" {
  description = "Enable or disable the daily AI summary schedule"
  type        = bool
  default     = true
}

variable "schedule_expression" {
  description = "Schedule expression for generating AI summaries (cron or rate expression)"
  type        = string
  default     = "rate(6 hours)" # Every 6 hours
}