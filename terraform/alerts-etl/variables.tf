variable "aws_region" {
  description = "AWS region"
  type        = string
  default = "eu-west-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "c20-power-monitor-alerts-etl-repo"
}

variable "service_name" {
  description = "Name of the Lambda function service"
  type        = string
  default     = "c20-power-monitor-lambda-alerts-pipeline"
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 30
}

variable "lambda_memory" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 512
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}

variable "db_secret_name" {
  description = "Name of the Secrets Manager secret containing DB credentials"
  type        = string
  default = "c20-power-monitor-db-credentials"
}

variable "lambda_subnet_ids" {
  description = "Subnet IDs for Lambda VPC configuration"
  type        = list(string)
}

variable "lambda_security_group_ids" {
  description = "Security group IDs for Lambda"
  type        = list(string)
}