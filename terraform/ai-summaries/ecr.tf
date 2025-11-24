# ecr.tf - ECR Repository for AI Summary Lambda Docker Image

# ==============================================================================
# ECR Repository
# ==============================================================================

resource "aws_ecr_repository" "ai_summary_repo" {
  name         = var.ecr_repository_name
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
    Purpose     = "AI Summary Lambda"
  }
}

# ==============================================================================
# Outputs
# ==============================================================================

output "ecr_repository_name" {
  description = "Name of the ECR repository"
  value       = aws_ecr_repository.ai_summary_repo.name
}

output "ecr_repository_url" {
  description = "URL of the ECR repository for Docker push commands"
  value       = aws_ecr_repository.ai_summary_repo.repository_url
}

output "ecr_repository_arn" {
  description = "ARN of the ECR repository"
  value       = aws_ecr_repository.ai_summary_repo.arn
}