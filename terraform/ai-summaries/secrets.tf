# secrets_manager.tf - Secrets Manager for OpenAI API Key

resource "aws_secretsmanager_secret" "openai_key" {
  name        = "c20-power-monitor-secrets-ai"
  description = "OpenAI API key for AI-powered energy data summaries"

  tags = {
    Project     = "PowerMonitor"
    Environment = "dev"
    Purpose     = "AI Summary Generation"
  }
}

resource "aws_secretsmanager_secret_version" "openai_key_version" {
  secret_id = aws_secretsmanager_secret.openai_key.id
  secret_string = jsonencode({
    OPENAI_API_KEY = var.openai_api_key
  })
}

# Data source to reference existing DB credentials secret

data "aws_secretsmanager_secret" "db_credentials" {
  arn = var.db_credentials_secret_arn
}

# ==============================================================================
# Outputs

output "openai_secret_arn" {
  description = "ARN of the OpenAI API key secret"
  value       = aws_secretsmanager_secret.openai_key.arn
  sensitive   = true
}

output "openai_secret_name" {
  description = "Name of the OpenAI API key secret"
  value       = aws_secretsmanager_secret.openai_key.name
}