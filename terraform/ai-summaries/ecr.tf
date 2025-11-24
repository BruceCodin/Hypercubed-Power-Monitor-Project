# ecr.tf - ECR Repository for AI Summary Lambda Docker Image

# ==============================================================================
# ECR Repository
# ==============================================================================

resource "aws_ecr_repository" "ai_summary_repo" {
  name         = var.ecr_repository_name
  force_delete = true  # Allows deletion even if images exist (use with caution in production)

  image_scanning_configuration {
    scan_on_push = true  # Automatically scan images for vulnerabilities
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
    Purpose     = "AI Summary Lambda"
  }
}

# ==============================================================================
# ECR Lifecycle Policy (Optional - keeps repository clean)
# ==============================================================================

resource "aws_ecr_lifecycle_policy" "ai_summary_repo_policy" {
  repository = aws_ecr_repository.ai_summary_repo.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 5 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 5
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
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