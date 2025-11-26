# variables.tf - Variables for Dashboard ECR

variable "aws_region" {
  description = "AWS region for deploying resources"
  type        = string
  default     = "eu-west-2"
}

variable "ecr_repository_name" {
  description = "Name of the ECR repository for dashboard Docker images"
  type        = string
  default     = "c20-power-monitor-dashboard-repo"
}

# ECS Configuration
variable "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  type        = string
  default     = "c20-power-monitor-dashboard-cluster"
}

variable "ecs_service_name" {
  description = "Name of the ECS service"
  type        = string
  default     = "c20-power-monitor-dashboard-service"
}

variable "ecs_task_family" {
  description = "Family name for the ECS task definition"
  type        = string
  default     = "c20-power-monitor-dashboard-task"
}

variable "ecs_task_cpu" {
  description = "CPU units for the ECS task (256, 512, 1024, 2048, 4096)"
  type        = string
  default     = "512"
}

variable "ecs_task_memory" {
  description = "Memory for the ECS task in MB"
  type        = string
  default     = "1024"
}

variable "container_port" {
  description = "Port exposed by the Streamlit container"
  type        = number
  default     = 8501
}

variable "desired_count" {
  description = "Desired number of ECS tasks"
  type        = number
  default     = 1
}

# S3 Configuration
variable "s3_bucket_name" {
  description = "S3 bucket name for dashboard data access"
  type        = string
  default     = "c20-power-monitor-s3"
}

# Database Configuration
variable "db_secret_arn" {
  description = "ARN of the Secrets Manager secret containing database credentials"
  type        = string
}

# VPC Configuration (you'll need to provide these or create VPC first)
variable "vpc_id" {
  description = "VPC ID for ECS deployment"
  type        = string
}

variable "public_subnet_ids" {
  description = "List of public subnet IDs for ECS tasks"
  type        = list(string)
}