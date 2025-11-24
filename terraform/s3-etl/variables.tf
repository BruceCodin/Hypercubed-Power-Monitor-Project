variable "aws_region" {
  description = "AWS region"
  type        = string
  default = "eu-west-2"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
  default = "c20-power-monitor-s3"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "c20-power-monitor-s3-etl-repo"
}

# Lambda Configuration
variable "lambda_function_name" {
  description = "Name of the Lambda function"
  type        = string
  default     = "c20-power-monitor-s3-etl-lambda"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 180  # 180 seconds
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 1024
}

# EventBridge Scheduler Configuration
variable "scheduler_enabled" {
  description = "Enable or disable the EventBridge scheduler"
  type        = bool
  default     = true
}

variable "secrets_manager_arn" {
  description = "ARN of the existing Secrets Manager secret containing DB credentials and API keys"
  type        = string
}