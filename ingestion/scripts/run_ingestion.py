"""
Main ingestion runner — uploads all banking sample data to S3.

This script simulates a daily batch ingestion run.
Run this manually or trigger via Airflow DAG.

Usage:
    python ingestion/scripts/run_ingestion.py
"""

from datetime import datetime, timezone
from pathlib import Path
from ingestion.scripts.s3_uploader import S3Uploader
from ingestion.scripts.logger import PipelineLogger

# ── Config ────────────────────────────────────────────────────────────────────

BUCKET_NAME = "banking-data-platform-raw-akshay"
DATA_DIR = Path("data/sample")

# Files to upload: (local_file, entity_name)
FILES_TO_UPLOAD = [
    (DATA_DIR / "customers.csv",    "customers"),
    (DATA_DIR / "accounts.csv",     "accounts"),
    (DATA_DIR / "transactions.csv", "transactions"),
    (DATA_DIR / "fraud_flags.csv",  "fraud_flags"),
]


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    with PipelineLogger("s3_raw_ingestion", run_date=str(datetime.now(timezone.utc).date())) as log:

        # Initialize uploader
        uploader = S3Uploader(bucket_name=BUCKET_NAME)

        # Run batch upload
        results = uploader.upload_batch(
            files=FILES_TO_UPLOAD,
            partition_date=datetime.now(timezone.utc),
        )

        # Save manifest
        uploader.save_manifest()

        # Verify each upload
        log.info("Verifying uploads in S3...")
        for result in results:
            if result["status"] == "SUCCESS":
                verified = uploader.verify_upload(result["s3_key"])
                if verified:
                    log.info(f"✓ Verified: {result['s3_key']}")
                else:
                    log.warning(f"✗ Missing: {result['s3_key']}")


if __name__ == "__main__":
    main()