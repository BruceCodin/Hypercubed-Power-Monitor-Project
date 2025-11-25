# ecr.tf - Elastic Container Registry for Dashboard

# ECR Repository
resource "aws_ecr_repository" "dashboard" {
  name                 = var.ecr_repository_name
  force_delete         = true
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }
}

# Outputs

output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = aws_ecr_repository.dashboard.repository_url
}

output "ecr_repository_arn" {
  description = "ARN of the ECR repository"
  value       = aws_ecr_repository.dashboard.arn
}

output "ecr_repository_name" {
  description = "Name of the ECR repository"
  value       = aws_ecr_repository.dashboard.name
}