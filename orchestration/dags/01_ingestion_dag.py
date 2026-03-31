"""
DAG 1: Ingestion Pipeline
Uploads raw banking CSV files from local/source system to S3 Bronze layer.

Schedule: Daily at midnight
SLA: 1 hour
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[2]))

from orchestration.dags.dag_config import (
    DEFAULT_ARGS, S3_RAW_BUCKET,
    TAGS_INGESTION
)
from ingestion.scripts.s3_uploader import S3Uploader
from ingestion.scripts.logger import PipelineLogger

# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id="banking_01_ingestion",
    description="Upload raw banking data to S3 Bronze layer",
    default_args=DEFAULT_ARGS,
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=TAGS_INGESTION,
    doc_md="""
    ## Banking Ingestion DAG
    Uploads raw CSV files to S3 with Hive-style partitioning.

    ### Tasks
    1. `start` — Pipeline start marker
    2. `validate_source_files` — Check all source files exist
    3. `upload_customers` — Upload customers CSV to S3
    4. `upload_accounts` — Upload accounts CSV to S3
    5. `upload_transactions` — Upload transactions CSV to S3
    6. `upload_fraud_flags` — Upload fraud flags CSV to S3
    7. `verify_uploads` — Verify all files landed in S3
    8. `end` — Pipeline end marker
    """,
) as dag:

    # ── Task Functions ────────────────────────────────────────

    def validate_source_files(**context):
        """Validate all source files exist before uploading."""
        with PipelineLogger("validate_source_files") as log:
            data_dir = Path("data/sample")
            required_files = [
                "customers.csv",
                "accounts.csv",
                "transactions.csv",
                "fraud_flags.csv",
            ]
            missing = []
            for f in required_files:
                if not (data_dir / f).exists():
                    missing.append(f)

            if missing:
                raise FileNotFoundError(
                    f"Missing source files: {missing}"
                )
            log.info(f"All {len(required_files)} source files validated")
            return {"validated_files": required_files}

    def upload_entity(entity: str, **context):
        """Upload a single entity CSV to S3."""
        with PipelineLogger(f"upload_{entity}") as log:
            uploader = S3Uploader(bucket_name=S3_RAW_BUCKET)
            filepath = Path(f"data/sample/{entity}.csv")
            result = uploader.upload_file(
                filepath=filepath,
                entity=entity,
                partition_date=datetime.now(timezone.utc),
            )
            context["ti"].xcom_push(
                key=f"{entity}_s3_key",
                value=result["s3_key"]
            )
            log.info(f"Uploaded {entity} → {result['s3_uri']}")
            return result

    def verify_uploads(**context):
        """Verify all uploaded files exist in S3."""
        with PipelineLogger("verify_uploads") as log:
            uploader = S3Uploader(bucket_name=S3_RAW_BUCKET)
            entities = [
                "customers", "accounts",
                "transactions", "fraud_flags"
            ]
            failed = []
            for entity in entities:
                s3_key = context["ti"].xcom_pull(
                    key=f"{entity}_s3_key"
                )
                if s3_key:
                    verified = uploader.verify_upload(s3_key)
                    if not verified:
                        failed.append(entity)
                    else:
                        log.info(f"Verified: {s3_key}")

            if failed:
                raise ValueError(
                    f"Upload verification failed for: {failed}"
                )
            log.info("All uploads verified successfully")
            uploader.save_manifest()

    # ── Tasks ─────────────────────────────────────────────────

    start = EmptyOperator(task_id="start")

    validate = PythonOperator(
        task_id="validate_source_files",
        python_callable=validate_source_files,
    )

    upload_customers = PythonOperator(
        task_id="upload_customers",
        python_callable=upload_entity,
        op_kwargs={"entity": "customers"},
    )

    upload_accounts = PythonOperator(
        task_id="upload_accounts",
        python_callable=upload_entity,
        op_kwargs={"entity": "accounts"},
    )

    upload_transactions = PythonOperator(
        task_id="upload_transactions",
        python_callable=upload_entity,
        op_kwargs={"entity": "transactions"},
    )

    upload_fraud_flags = PythonOperator(
        task_id="upload_fraud_flags",
        python_callable=upload_entity,
        op_kwargs={"entity": "fraud_flags"},
    )

    verify = PythonOperator(
        task_id="verify_uploads",
        python_callable=verify_uploads,
    )

    end = EmptyOperator(task_id="end")

    # ── Task Dependencies ─────────────────────────────────────
    start >> validate
    validate >> [
        upload_customers,
        upload_accounts,
        upload_transactions,
        upload_fraud_flags,
    ]
    [
        upload_customers,
        upload_accounts,
        upload_transactions,
        upload_fraud_flags,
    ] >> verify >> end