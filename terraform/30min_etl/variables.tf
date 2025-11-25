variable "aws_region" {
  description = "AWS region for deploying resources"
  type        = string
  default     = "eu-west-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository for 30_min ETL Lambda Docker image"
  type        = string
  default     = "c20-power-monitor-30min-etl-repo"
}