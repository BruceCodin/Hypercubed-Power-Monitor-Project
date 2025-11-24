# Reference the existing S3 bucket created by s3-power-cut-etl module
data "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
}