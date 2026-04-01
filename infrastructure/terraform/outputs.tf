# ============================================================
# Banking Data Platform — Terraform Outputs
# ============================================================

output "s3_raw_bucket_name" {
  description = "Name of the raw S3 bucket"
  value       = aws_s3_bucket.raw.id
}

output "s3_raw_bucket_arn" {
  description = "ARN of the raw S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "s3_processed_bucket_name" {
  description = "Name of the processed S3 bucket"
  value       = aws_s3_bucket.processed.id
}

output "snowflake_iam_role_arn" {
  description = "ARN of the Snowflake IAM role"
  value       = aws_iam_role.snowflake_s3.arn
}

output "pipeline_role_arn" {
  description = "ARN of the pipeline execution role"
  value       = aws_iam_role.pipeline.arn
}

output "aws_region" {
  description = "AWS region"
  value       = var.aws_region
}