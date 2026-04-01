# ============================================================
# Banking Data Platform — S3 Buckets
# ============================================================

# ── Raw (Bronze) Bucket ───────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = var.s3_raw_bucket_name

  tags = {
    Name    = "${var.project_name}-raw"
    Layer   = "bronze"
    Purpose = "Raw data landing zone"
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "raw" {
  bucket                  = aws_s3_bucket.raw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "raw-data-retention"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    # Move to cheaper storage after 90 days
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    # Archive after 365 days
    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    # Delete after 7 years (SOX compliance)
    expiration {
      days = var.s3_raw_retention_days
    }
  }

  rule {
    id     = "logs-retention"
    status = "Enabled"

    filter {
      prefix = "logs/"
    }

    expiration {
      days = 30
    }
  }
}

# ── Processed Bucket ──────────────────────────────────────────
resource "aws_s3_bucket" "processed" {
  bucket = var.s3_processed_bucket_name

  tags = {
    Name    = "${var.project_name}-processed"
    Layer   = "processed"
    Purpose = "Processed data storage"
  }
}

resource "aws_s3_bucket_versioning" "processed" {
  bucket = aws_s3_bucket.processed.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "processed" {
  bucket = aws_s3_bucket.processed.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "processed" {
  bucket                  = aws_s3_bucket.processed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── DAGs Folder (for MWAA) ────────────────────────────────────
resource "aws_s3_object" "dags_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "dags/"
  content = ""
}

resource "aws_s3_object" "plugins_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "plugins/"
  content = ""
}