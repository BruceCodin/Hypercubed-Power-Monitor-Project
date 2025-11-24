provider "aws" {
  region     = var.aws_region
}

# ------ S3 Bucket ------
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
}

resource "aws_s3_object" "folders" {
    # We can use for_each to create multiple folders at once
  for_each = toset(["power_cuts/", "power_generation/", "athena_output/"])

  bucket  = aws_s3_bucket.data_bucket.id
  key     = each.value
  content = "" # Empty content
}


# ------ Outputs ------
output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.data_bucket.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.data_bucket.arn
}
