variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository"
  type        = string
  default     = "c20-power-monitor-neso-etl-repo"
}

variable "db_secret_name" {
  description = "Name of the Secrets Manager secret containing DB credentials"
  type        = string
  default     = "c20-power-monitor-db-credentials"
}

variable "lambda_timeout" {
  description = "Lambda function timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory" {
  description = "Lambda function memory in MB"
  type        = number
  default     = 512
}

variable "lambda_subnet_ids" {
  description = "Subnet IDs for Lambda VPC configuration (must match RDS subnets)"
  type        = list(string)
  default     = ["subnet-0c47ef6fc81ba084a", "subnet-0c2e92c1b7b782543"]
}

variable "lambda_security_group_ids" {
  description = "Security group IDs for Lambda (must allow access to RDS)"
  type        = list(string)
  default     = ["sg-0e6b102a45b28050d"]
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 14
}