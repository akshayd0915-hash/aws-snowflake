"""
Shared configuration for all Banking Data Platform DAGs.
Centralizes common settings, defaults and constants.
"""

from datetime import datetime, timedelta

# ── Default Arguments ─────────────────────────────────────────
# Applied to every task in every DAG unless overridden
DEFAULT_ARGS = {
    "owner":                  "banking-data-platform",
    "depends_on_past":        False,
    "start_date":             datetime(2024, 1, 1),
    "email":                  ["akshaydubey401@email.com"],
    "email_on_failure":       True,
    "email_on_retry":         False,
    "retries":                3,
    "retry_delay":            timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":        timedelta(minutes=30),
    "execution_timeout":      timedelta(hours=2),
}

# ── Snowflake Connection ──────────────────────────────────────
SNOWFLAKE_CONN_ID    = "snowflake_banking"
SNOWFLAKE_WAREHOUSE  = "BANKING_WH"
SNOWFLAKE_DATABASE   = "BANKING_DB"
SNOWFLAKE_ROLE       = "ACCOUNTADMIN"

# ── AWS Connection ────────────────────────────────────────────
AWS_CONN_ID          = "aws_banking"
S3_RAW_BUCKET        = "banking-data-platform-raw-akshay"
S3_PROCESSED_BUCKET  = "banking-data-platform-processed-akshay"

# ── Pipeline Config ───────────────────────────────────────────
BRONZE_SCHEMA        = "RAW"
SILVER_SCHEMA        = "SILVER"
GOLD_SCHEMA          = "GOLD"
AUDIT_SCHEMA         = "AUDIT"

# ── SLA Config ────────────────────────────────────────────────
BRONZE_SLA           = timedelta(hours=1)
SILVER_SLA           = timedelta(hours=2)
GOLD_SLA             = timedelta(hours=3)

# ── Tags ──────────────────────────────────────────────────────
TAGS_INGESTION       = ["banking", "ingestion", "bronze", "s3"]
TAGS_SILVER          = ["banking", "transformation", "silver", "snowflake"]
TAGS_GOLD            = ["banking", "dimensional", "gold", "snowflake"]
TAGS_QUALITY         = ["banking", "data-quality", "great-expectations"]
TAGS_MASTER          = ["banking", "master", "end-to-end"]