resource "aws_s3_bucket" "bucket" {
  bucket = "fahatobo_bucket"

  tags = local.tags
}