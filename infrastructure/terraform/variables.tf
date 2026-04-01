# ============================================================
# Banking Data Platform — Terraform Variables
# ============================================================

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "development"

  validation {
    condition     = contains(["development", "uat", "production"], var.environment)
    error_message = "Environment must be development, uat, or production."
  }
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "banking-data-platform"
}

variable "owner_name" {
  description = "Owner name for bucket naming"
  type        = string
  default     = "akshay"
}

variable "snowflake_account_id" {
  description = "Snowflake AWS account ID for IAM trust"
  type        = string
  default     = "490333984697"
}

variable "snowflake_external_id" {
  description = "Snowflake external ID for IAM role"
  type        = string
  sensitive   = true
}

variable "s3_raw_bucket_name" {
  description = "S3 raw (Bronze) bucket name"
  type        = string
  default     = "banking-data-platform-raw-akshay"
}

variable "s3_processed_bucket_name" {
  description = "S3 processed bucket name"
  type        = string
  default     = "banking-data-platform-processed-akshay"
}

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

variable "s3_raw_retention_days" {
  description = "S3 raw data retention in days"
  type        = number
  default     = 2555 # 7 years for SOX compliance
}