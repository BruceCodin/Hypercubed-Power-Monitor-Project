output "database_name" {
  value       = aws_db_instance.c20_power_monitor_db.db_name
  description = "Database name"
}

output "instance_id" {
  value       = aws_db_instance.c20_power_monitor_db.identifier
  description = "RDS instance identifier"
}

output "endpoint" {
  value       = aws_db_instance.c20_power_monitor_db.endpoint
  description = "RDS instance endpoint"
}

output "db_username" {
  value       = aws_db_instance.c20_power_monitor_db.username
  description = "Database master username"
}