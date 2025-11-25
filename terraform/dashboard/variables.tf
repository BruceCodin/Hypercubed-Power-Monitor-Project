# variables.tf - Variables for Dashboard ECR

variable "aws_region" {
  description = "AWS region for deploying resources"
  type        = string
  default     = "eu-west-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository for dashboard Docker images"
  type        = string
  default     = "c20-power-monitor-dashboard"
}

variable "image_retention_count" {
  description = "Number of images to retain in ECR"
  type        = number
  default     = 5
}