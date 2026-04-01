# ============================================================
# Banking Data Platform — IAM Roles and Policies
# ============================================================

# ── Snowflake S3 Integration Role ─────────────────────────────
data "aws_iam_policy_document" "snowflake_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.snowflake_account_id}:root"]
    }

    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.snowflake_external_id]
    }
  }
}

resource "aws_iam_role" "snowflake_s3" {
  name               = "snowflake-s3-role"
  assume_role_policy = data.aws_iam_policy_document.snowflake_trust.json
  description        = "Allows Snowflake to access S3 banking buckets"

  tags = {
    Purpose = "Snowflake S3 integration"
  }
}

data "aws_iam_policy_document" "snowflake_s3_policy" {
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      "${aws_s3_bucket.raw.arn}/*",
      "${aws_s3_bucket.processed.arn}/*"
    ]
  }

  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      aws_s3_bucket.processed.arn
    ]
  }
}

resource "aws_iam_policy" "snowflake_s3" {
  name        = "snowflake-s3-banking-policy"
  description = "Allows Snowflake to read/write banking S3 buckets"
  policy      = data.aws_iam_policy_document.snowflake_s3_policy.json
}

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.snowflake_s3.arn
}

# ── Pipeline Execution Role ───────────────────────────────────
resource "aws_iam_role" "pipeline" {
  name        = "${var.project_name}-pipeline-role"
  description = "Role for banking data pipeline execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "ec2.amazonaws.com" }
        Action    = "sts:AssumeRole"
      }
    ]
  })
}

data "aws_iam_policy_document" "pipeline_policy" {
  # S3 access
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.processed.arn,
      "${aws_s3_bucket.processed.arn}/*"
    ]
  }

  # CloudWatch Logs
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }

  # Secrets Manager — for Snowflake credentials
  statement {
    effect = "Allow"
    actions = [
      "secretsmanager:GetSecretValue"
    ]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:*:secret:banking-*"
    ]
  }
}

resource "aws_iam_policy" "pipeline" {
  name   = "${var.project_name}-pipeline-policy"
  policy = data.aws_iam_policy_document.pipeline_policy.json
}

resource "aws_iam_role_policy_attachment" "pipeline" {
  role       = aws_iam_role.pipeline.name
  policy_arn = aws_iam_policy.pipeline.arn
}