# Changelog

All notable changes to the Banking Data Platform are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [1.0.0] — 2026-03-31

### 🎉 Initial Release — Full Medallion Architecture

#### Infrastructure
- AWS S3 buckets created — `banking-data-platform-raw-akshay` and `banking-data-platform-processed-akshay`
- Snowflake database `BANKING_DB` with RAW, SILVER, GOLD, AUDIT schemas
- Snowflake warehouse `BANKING_WH` (X-SMALL, auto-suspend 60s)
- IAM roles and policies for Snowflake → S3 storage integration
- S3 external stage `S3_RAW_STAGE` with CSV file format

#### Ingestion
- Python `S3Uploader` module with MD5 checksums, retry logic, upload manifests
- Hive-style S3 partitioning (`year=/month=/day=`)
- Pydantic data contracts for all 4 banking entities
- Structured logging with `loguru` — rotating daily files, separate error log
- Sample data generator — 500 customers, 1,026 accounts, 5,741 transactions, 487 fraud flags

#### Bronze Layer
- `RAW_CUSTOMERS` — 500 records loaded from S3
- `RAW_ACCOUNTS` — 1,026 records loaded from S3
- `RAW_TRANSACTIONS` — 5,741 records loaded from S3
- `RAW_FRAUD_FLAGS` — 487 records loaded from S3
- All columns VARCHAR — exact replica of source data
- Pipeline metadata columns — `_source_file`, `_batch_id`, `_load_timestamp`

#### Silver Layer
- `SILVER_CUSTOMERS` — type cast, cleaned, credit tier derived, tenure calculated
- `SILVER_ACCOUNTS` — balance typed, is_overdrawn flag, account age calculated
- `SILVER_TRANSACTIONS` — amount typed, is_debit/is_credit flags, date key generated
- `SILVER_FRAUD_FLAGS` — severity ranked, days_to_review calculated
- Deduplication via `ROW_NUMBER()` window function on all tables
- DQ flags — `dq_is_valid` and `dq_issues` columns on all tables
- 100% records passed validation (0 invalid records)

#### Gold Layer (Star Schema)
- `DIM_DATE` — 2,922 rows covering 2020-2027
- `DIM_CUSTOMERS` — 500 rows with age band, tenure band, credit tier
- `DIM_ACCOUNTS` — 1,026 rows with account type description, age band
- `FACT_TRANSACTIONS` — 5,741 rows with surrogate keys, fraud flag
- `FACT_FRAUD` — 487 rows with fraud score band, SLA tracking

#### Orchestration
- `banking_00_master_pipeline` — end-to-end DAG, daily 11 PM
- `banking_01_ingestion` — S3 upload DAG, daily 12 AM
- `banking_02_bronze_to_silver` — ELT transformation DAG, daily 1 AM
- `banking_03_silver_to_gold` — dimensional model DAG, daily 3 AM
- All DAGs with retries, exponential backoff, idempotency, SLAs

#### Data Quality
- 19 automated DQ checks across Silver and Gold layers
- 12 Silver checks — row counts, nulls, duplicates, business rules
- 7 Gold checks — row counts, referential integrity, duplicates
- 100% pass rate on first run
- JSON reports saved to `logs/` with timestamps

#### CI/CD
- `pr_checks.yml` — linting, unit tests, DAG validation, security scan
- `deploy_pipeline.yml` — DAG deploy to S3, Snowflake object validation
- `daily_dq.yml` — scheduled DQ checks at 6 AM UTC

#### Documentation
- Architecture SVG diagram — end-to-end pipeline visualization
- Rich README with badges, data model, regulatory context
- Pydantic schemas as data contracts

---

## [0.1.0] — 2026-03-30

### 🏗️ Project Scaffold

- Initial repository structure
- Python project setup — `pyproject.toml`, virtual environment
- Centralized config management with `pydantic-settings`
- `.gitignore` — secrets and credentials excluded
- Base folder structure — ingestion, transformation, orchestration, tests