variable "aws_region" {
  description = "AWS region"
  type        = string
  default = "eu-west-2"
}


variable "db_host" {
  description = "Database host endpoint"
  type        = string
  sensitive   = true
}

variable "db_port" {
  description = "Database port"
  type        = number
}

variable "db_name" {
  description = "Database name"
  type        = string
  sensitive   = true
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

variable "db_schema" {
  description = "Database schema"
  type        = string
  sensitive   = true
}
