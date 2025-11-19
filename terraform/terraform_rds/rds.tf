terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
}

# DB Subnet Group
resource "aws_db_subnet_group" "power_monitor_subnet_group" {
  name       = "power-monitor-db-subnet-group"
  subnet_ids = var.subnet_ids

  tags = {
    Name = "power-monitor-db-subnet-group"
  }
}

# Security Group
resource "aws_security_group" "rds_sg" {
  name        = "power-monitor-rds-sg"
  description = "Allow PostgreSQL traffic"
  vpc_id      = var.vpc_id  

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "power-monitor-rds-sg"
  }
}

# RDS Instance
resource "aws_db_instance" "c20_power_monitor_db" {
  identifier          = "power-monitor-db"
  engine              = "postgres"
  engine_version      = "15.15"
  instance_class      = "db.t3.micro"
  allocated_storage   = 20
  storage_type        = "gp2"
  storage_encrypted   = true

  db_name  = "powermonitordb"
  username = var.db_username
  password = var.db_password
  backup_retention_period = 7

  publicly_accessible    = true
  skip_final_snapshot    = true
  db_subnet_group_name   = aws_db_subnet_group.power_monitor_subnet_group.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]

  # CloudWatch Logs
  enabled_cloudwatch_logs_exports = ["postgresql", "upgrade"]

  tags = {
    Name = "power-monitor-db"
  }
}