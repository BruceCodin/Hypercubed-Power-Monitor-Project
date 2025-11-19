provider "aws" {
  region     = var.aws_region
}

resource "aws_ecr_repository" "power-monitor-repo" {
  name                 = var.ecr_repository_name
  force_delete = true # useful while in development
}