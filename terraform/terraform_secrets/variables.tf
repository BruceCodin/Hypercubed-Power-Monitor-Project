variable "aws_region" {
  description = "AWS region"
  type        = string
  default = "eu-west-2"
}

variable "db_secret_name" {
  description = "Name of the Secrets Manager secret containing DB credentials"
  type        = string
  default = "c20-power-monitor-db-credentials"
}

variable "db_host" {
  description = "Database host endpoint"
  type        = string
}

variable "db_port" {
  description = "Database port"
  type        = number
}

variable "db_name" {
  description = "Database name"
  type        = string
}

variable "db_user" {
  description = "Database username"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}
