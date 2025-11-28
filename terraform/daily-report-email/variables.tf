# variables.tf - Configuration variables for Daily Report Email infrastructure
# ==============================================================================

# AWS Configuration
# ==============================================================================

variable "aws_region" {
  description = "AWS region for deploying resources"
  type        = string
  default     = "eu-west-2"
}

# Project & Environment Tags
# ==============================================================================

variable "project_name" {
  description = "Project name for resource naming and tagging"
  type        = string
  default     = "c20-power-monitor"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# ECR Configuration
# ==============================================================================

variable "ecr_repository_name" {
  description = "Name of the ECR repository for summaries pipeline Lambda Docker image"
  type        = string
  default     = "c20-power-monitor-summaries-repo"
}

# Lambda Configuration
# ==============================================================================

variable "lambda_function_name" {
  description = "Name of the Lambda function for daily report email"
  type        = string
  default     = "c20-power-monitor-daily-report-email-lambda"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds (max 900 for Lambda)"
  type        = number
  default     = 300 # 5 minutes for email generation and sending

  validation {
    condition     = var.lambda_timeout > 0 && var.lambda_timeout <= 900
    error_message = "Lambda timeout must be between 1 and 900 seconds."
  }
}

variable "lambda_memory_size" {
  description = "Lambda memory allocation in MB (128 MB to 10,240 MB)"
  type        = number
  default     = 512

  validation {
    condition     = var.lambda_memory_size >= 128 && var.lambda_memory_size <= 10240
    error_message = "Lambda memory must be between 128 and 10,240 MB."
  }
}

variable "lambda_log_retention_days" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 7
}

# Step Functions Configuration
# ==============================================================================

variable "step_function_name" {
  description = "Name of the Step Functions state machine"
  type        = string
  default     = "c20-power-monitor-daily-report-workflow"
}

# SES Configuration
# ==============================================================================

variable "ses_enabled" {
  description = "Enable SES email sending (must be enabled in AWS account and verified)"
  type        = bool
  default     = true
}

variable "sender_email" {
  description = "Email address to send reports from (must be verified in SES)"
  type        = string

  validation {
    condition     = can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", var.sender_email))
    error_message = "sender_email must be a valid email address."
  }
}

variable "recipient_emails" {
  description = "List of recipient email addresses for daily reports"
  type        = list(string)

  validation {
    condition = alltrue([
      for email in var.recipient_emails : can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email))
    ])
    error_message = "All recipient_emails must be valid email addresses."
  }
}

variable "email_subject" {
  description = "Subject line for daily report email"
  type        = string
  default     = "Daily Power Monitor Report - Watt's Up"
}

# S3 Configuration (for summaries)
# ==============================================================================

variable "summaries_bucket_name" {
  description = "Name of S3 bucket containing AI-generated summaries"
  type        = string
  default     = "c20-power-monitor-s3"
}

variable "summaries_bucket_key" {
  description = "S3 key path for latest summary JSON file"
  type        = string
  default     = "summaries/summary-latest.json"
}

# Secrets Manager Configuration
# ==============================================================================

variable "db_credentials_secret_arn" {
  description = "ARN of Secrets Manager secret containing database credentials"
  type        = string
  sensitive   = true
}

# Scheduling Configuration
# ==============================================================================

variable "schedule_enabled" {
  description = "Enable or disable the daily report email schedule"
  type        = bool
  default     = true
}

variable "schedule_expression" {
  description = "Schedule expression for sending daily reports (cron or rate expression)"
  type        = string
  default     = "cron(9 6 * * ? *)" # 6:09 AM UTC daily
}

variable "schedule_timezone" {
  description = "Timezone for schedule expression (IANA timezone)"
  type        = string
  default     = "UTC"
}

# Common Tags
# ==============================================================================

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "PowerMonitor"
    Component   = "DailyReports"
    Environment = "dev"
    ManagedBy   = "Terraform"
  }
}
