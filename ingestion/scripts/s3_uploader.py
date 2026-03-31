"""
S3 Upload Module for Banking Data Platform.
Handles uploading raw banking data files to S3 Bronze layer.

Features:
    - Partitioned S3 paths (year/month/day)
    - Retry logic with exponential backoff
    - File validation before upload
    - Upload manifest for tracking
    - MD5 checksum verification

Usage:
    from ingestion.scripts.s3_uploader import S3Uploader
    uploader = S3Uploader(bucket_name="banking-data-platform-raw-akshay")
    uploader.upload_file("data/sample/customers.csv", "customers")
"""

import boto3
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from botocore.exceptions import ClientError, NoCredentialsError
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)
import logging
from ingestion.scripts.logger import get_logger

logger = get_logger(__name__)


# ── Constants ─────────────────────────────────────────────────────────────────

VALID_EXTENSIONS = {".csv", ".json", ".parquet"}
MAX_FILE_SIZE_MB = 500
MANIFEST_FILENAME = "upload_manifest.json"


# ── Helpers ───────────────────────────────────────────────────────────────────

def compute_md5(filepath: Path) -> str:
    """Compute MD5 checksum of a file for integrity verification."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def build_s3_key(
    entity: str,
    filename: str,
    partition_date: Optional[datetime] = None
) -> str:
    """
    Build partitioned S3 key following Hive-style partitioning.

    Pattern:
        raw/{entity}/year={YYYY}/month={MM}/day={DD}/{filename}

    Example:
        raw/transactions/year=2024/month=01/day=15/transactions.csv

    Args:
        entity: Data entity name e.g. customers, transactions
        filename: File name with extension
        partition_date: Date to partition by, defaults to today

    Returns:
        Full S3 key string
    """
    if partition_date is None:
        partition_date = datetime.now(timezone.utc)

    return (
        f"raw/{entity}/"
        f"year={partition_date.year}/"
        f"month={partition_date.month:02d}/"
        f"day={partition_date.day:02d}/"
        f"{filename}"
    )


# ── Main Class ────────────────────────────────────────────────────────────────

class S3Uploader:
    """
    Enterprise S3 upload client for Banking Data Platform.

    Handles all raw data uploads to the Bronze (raw) S3 layer
    with validation, retries, checksums, and manifest tracking.
    """

    def __init__(
        self,
        bucket_name: str,
        region: str = "us-east-1",
    ):
        self.bucket_name = bucket_name
        self.region = region
        self._manifest: list[dict] = []

        try:
            self.s3_client = boto3.client("s3", region_name=region)
            logger.info(
                f"S3Uploader initialized",
                bucket=bucket_name,
                region=region
            )
        except NoCredentialsError:
            logger.error("AWS credentials not found. Run 'aws configure'.")
            raise

    # ── Validation ────────────────────────────────────────────────────────────

    def _validate_file(self, filepath: Path) -> None:
        """
        Validate file before upload.
        Checks existence, extension, and file size.
        """
        if not filepath.exists():
            raise FileNotFoundError(f"File not found: {filepath}")

        if filepath.suffix not in VALID_EXTENSIONS:
            raise ValueError(
                f"Invalid file type: {filepath.suffix}. "
                f"Allowed: {VALID_EXTENSIONS}"
            )

        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        if file_size_mb > MAX_FILE_SIZE_MB:
            raise ValueError(
                f"File too large: {file_size_mb:.1f}MB. "
                f"Max allowed: {MAX_FILE_SIZE_MB}MB"
            )

        logger.debug(
            f"File validation passed",
            file=str(filepath),
            size_mb=round(file_size_mb, 2)
        )

    # ── Upload with Retry ─────────────────────────────────────────────────────

    @retry(
        retry=retry_if_exception_type(ClientError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        before_sleep=before_sleep_log(logging.getLogger(__name__), logging.WARNING),
        reraise=True,
    )
    def _upload_to_s3(
        self,
        filepath: Path,
        s3_key: str,
        metadata: dict,
    ) -> None:
        """Internal upload method with automatic retry on failure."""
        self.s3_client.upload_file(
            Filename=str(filepath),
            Bucket=self.bucket_name,
            Key=s3_key,
            ExtraArgs={
                "Metadata": metadata,
                "ContentType": "text/csv" if filepath.suffix == ".csv" else "application/json",
                "ServerSideEncryption": "AES256",  # Encrypt at rest
            }
        )

    # ── Public Interface ──────────────────────────────────────────────────────

    def upload_file(
        self,
        filepath: str | Path,
        entity: str,
        partition_date: Optional[datetime] = None,
    ) -> dict:
        """
        Upload a single file to S3 Bronze layer.

        Args:
            filepath: Local path to file
            entity: Data entity e.g. customers, transactions
            partition_date: Override partition date (defaults to today)

        Returns:
            Upload result dict with s3_key, checksum, status
        """
        filepath = Path(filepath)
        self._validate_file(filepath)

        # Build S3 key with partitioning
        s3_key = build_s3_key(
            entity=entity,
            filename=filepath.name,
            partition_date=partition_date,
        )

        # Compute checksum before upload
        checksum = compute_md5(filepath)

        # Metadata stored alongside the file in S3
        metadata = {
            "source-entity": entity,
            "md5-checksum": checksum,
            "uploaded-at": datetime.now(timezone.utc).isoformat(),
            "pipeline": "banking-data-platform",
            "environment": "development",
        }

        logger.info(
            f"Uploading {filepath.name} → s3://{self.bucket_name}/{s3_key}"
        )

        try:
            self._upload_to_s3(filepath, s3_key, metadata)

            result = {
                "status": "SUCCESS",
                "entity": entity,
                "local_file": str(filepath),
                "s3_bucket": self.bucket_name,
                "s3_key": s3_key,
                "s3_uri": f"s3://{self.bucket_name}/{s3_key}",
                "checksum_md5": checksum,
                "file_size_bytes": filepath.stat().st_size,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
            }

            self._manifest.append(result)
            logger.success(
                f"Upload successful → s3://{self.bucket_name}/{s3_key}"
            )
            return result

        except ClientError as e:
            error_result = {
                "status": "FAILED",
                "entity": entity,
                "local_file": str(filepath),
                "error": str(e),
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
            }
            self._manifest.append(error_result)
            logger.error(f"Upload failed for {filepath.name}: {e}")
            raise

    def upload_batch(
        self,
        files: list[tuple[str, str]],
        partition_date: Optional[datetime] = None,
    ) -> list[dict]:
        """
        Upload multiple files in sequence.

        Args:
            files: List of (filepath, entity) tuples
            partition_date: Override partition date for all files

        Returns:
            List of upload result dicts

        Example:
            uploader.upload_batch([
                ("data/sample/customers.csv", "customers"),
                ("data/sample/transactions.csv", "transactions"),
            ])
        """
        results = []
        total = len(files)

        logger.info(f"Starting batch upload of {total} files")

        for i, (filepath, entity) in enumerate(files, 1):
            logger.info(f"Processing file {i}/{total}: {filepath}")
            try:
                result = self.upload_file(filepath, entity, partition_date)
                results.append(result)
            except Exception as e:
                logger.error(f"Skipping {filepath} due to error: {e}")
                continue

        success = sum(1 for r in results if r["status"] == "SUCCESS")
        failed = total - success

        logger.info(
            f"Batch upload complete — "
            f"Success: {success}/{total}, Failed: {failed}/{total}"
        )

        return results

    def save_manifest(self, output_path: str = "logs/upload_manifest.json") -> None:
        """
        Save upload manifest to local JSON file.
        Manifest tracks all uploads for auditing and idempotency checks.
        """
        manifest_path = Path(output_path)
        manifest_path.parent.mkdir(exist_ok=True)

        manifest = {
            "pipeline": "banking-data-platform",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_files": len(self._manifest),
            "successful": sum(1 for r in self._manifest if r["status"] == "SUCCESS"),
            "failed": sum(1 for r in self._manifest if r["status"] == "FAILED"),
            "uploads": self._manifest,
        }

        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)

        logger.info(f"Upload manifest saved to {manifest_path}")

    def verify_upload(self, s3_key: str) -> bool:
        """
        Verify a file exists in S3 after upload.

        Args:
            s3_key: S3 object key to verify

        Returns:
            True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            logger.debug(f"Verified: s3://{self.bucket_name}/{s3_key} exists")
            return True
        except ClientError:
            logger.warning(f"Verification failed: s3://{self.bucket_name}/{s3_key}")
            return False