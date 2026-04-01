# ============================================================
# Banking Data Platform — Terraform Main Configuration
# Provisions all AWS resources for the pipeline
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state — S3 backend
  backend "s3" {
    bucket = "banking-data-platform-raw-akshay"
    key    = "terraform/state/banking-platform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "banking-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "data-engineering"
    }
  }
}