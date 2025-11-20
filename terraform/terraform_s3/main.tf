provider "aws" {
  region     = var.aws_region
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
}

output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.data_bucket.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.data_bucket.arn
}
