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

variable "project_name" {
  description = "Project name for tagging"
  type        = string
  default     = "PowerMonitor"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}