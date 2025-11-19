provider "aws" {
  region     = var.aws_region
}

resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
}