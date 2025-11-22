resource "aws_ecr_repository" "power_monitor_repo" {
  name         = var.ecr_repository_name
  force_delete = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
  }
}

# Outputs for ECR repository
output "ecr_repository_name" {
  description = "ECR repository name"
  value       = aws_ecr_repository.power_monitor_repo.name
}

output "ecr_repository_url" {
  description = "ECR repository URL for Docker push"
  value       = aws_ecr_repository.power_monitor_repo.repository_url
}

