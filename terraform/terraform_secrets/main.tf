provider "aws" {
  region     = var.aws_region
}

resource "aws_secretsmanager_secret" "db_credentials" {
  name        = "power-monitor-db-credentials"
  description = "Database credentials for power monitor RDS"
}

resource "aws_secretsmanager_secret_version" "db_credentials" {
  secret_id = aws_secretsmanager_secret.db_credentials.id
  secret_string = jsonencode({
    DB_HOST     = var.db_host
    DB_PORT     = var.db_port
    DB_NAME     = var.db_name
    DB_USER     = var.db_user
    DB_PASSWORD = var.db_password
  })
}
