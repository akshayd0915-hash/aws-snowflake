# CLAUDE.md — Banking Data Platform

This file configures Claude Code's behavior when working in this repository.

---

## Project Overview

This is an enterprise-grade banking data platform built on AWS and Snowflake.
It implements a full Medallion Architecture (Bronze → Silver → Gold) with
Apache Airflow orchestration, automated data quality checks, and CI/CD via
GitHub Actions.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Cloud | AWS (S3, MWAA, Glue, Lambda) |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow 2.8+ |
| Language | Python 3.12 |
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Data Quality | Custom DQ Framework |
| Config | Pydantic Settings |
| Logging | Loguru |

---

## Architecture Principles

### 1. Medallion Architecture
- **Bronze** — exact replica of source, all VARCHAR, never modified, append-only
- **Silver** — typed, cleaned, validated, deduplicated — single source of truth
- **Gold** — business-level star schema for analytics consumption

### 2. ELT Not ETL
All transformation logic lives in Snowflake SQL — not in Python.
Python handles ingestion and orchestration only.

### 3. Idempotency
Every pipeline run must produce the same result regardless of how many
times it runs. Use TRUNCATE + reload for Silver/Gold. Use Snowflake
COPY INTO load history for Bronze idempotency.

### 4. Data Contracts
All source data must conform to Pydantic schemas in
`ingestion/schemas/banking_schemas.py` before ingestion.

---

## Code Standards

### Python
- All Python code must be type-hinted
- Use `loguru` for all logging — never use `print()`
- Use `pydantic` for all data validation and config
- Use `tenacity` for retry logic — never write manual retry loops
- All functions must have docstrings
- Line length: 88 characters (Black formatter)

### SQL
- All SQL is Snowflake dialect
- Use fully qualified table names: `BANKING_DB.SCHEMA.TABLE`
- Bronze tables — all VARCHAR columns, no transforms
- Silver tables — use `TRY_TO_*` functions for safe type casting
- Gold tables — use surrogate keys via Snowflake SEQUENCE
- Never use `SELECT *` in production queries
- Always include `WHERE dq_is_valid = TRUE` when reading from Silver

### Airflow DAGs
- All DAGs must have `max_active_runs=1`
- All DAGs must have `catchup=False`
- All tasks must have retry logic via `DEFAULT_ARGS`
- Use `EmptyOperator` for start/end markers
- Use `TriggerDagRunOperator` in master DAG only
- Document all DAGs with `doc_md`

---

## Directory Structure Rules

| Directory | Purpose | Rules |
|---|---|---|
| `ingestion/schemas/` | Pydantic data contracts | One schema per entity |
| `ingestion/scripts/` | Pipeline Python scripts | No SQL here |
| `transformation/snowflake/bronze/` | Bronze SQL | DDL + COPY INTO only |
| `transformation/snowflake/silver/` | Silver SQL | Transforms + DQ flags |
| `transformation/snowflake/gold/` | Gold SQL | Star schema only |
| `orchestration/dags/` | Airflow DAGs | Python only, no SQL |
| `data_quality/expectations/` | DQ checks | Snowflake queries only |
| `configs/` | Environment configs | Never commit secrets |
| `tests/unit/` | Unit tests | No Snowflake/AWS calls |
| `tests/integration/` | Integration tests | Requires credentials |

---

## Snowflake Conventions

### Naming
- Bronze tables: `RAW_<ENTITY>` e.g. `RAW_CUSTOMERS`
- Silver tables: `SILVER_<ENTITY>` e.g. `SILVER_CUSTOMERS`
- Gold dimensions: `DIM_<ENTITY>` e.g. `DIM_CUSTOMERS`
- Gold facts: `FACT_<ENTITY>` e.g. `FACT_TRANSACTIONS`
- Sequences: `SEQ_<ENTITY>_KEY` e.g. `SEQ_CUSTOMER_KEY`
- Stages: `S3_<LAYER>_STAGE` e.g. `S3_RAW_STAGE`

### Pipeline Metadata Columns
All Bronze and Silver tables must include:
- `_source_file` — S3 source file path
- `_batch_id` — Pipeline batch identifier
- `_load_timestamp` / `_silver_loaded_at` / `_gold_loaded_at`

### Data Quality Columns
All Silver tables must include:
- `dq_is_valid BOOLEAN` — TRUE if record passes all checks
- `dq_issues VARCHAR(500)` — Description of any DQ issues

---

## Banking Domain Context

### Entities
- **Customers** — retail banking customers with KYC attributes
- **Accounts** — CHECKING, SAVINGS, LOAN, CREDIT accounts
- **Transactions** — DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT, FEE
- **Fraud Flags** — ML-scored suspicious transaction markers

### Regulatory Drivers
- **BSA/AML** — KYC status tracking, transaction monitoring, fraud scoring
- **GLBA** — PII field classification, email/phone governance
- **SOX** — Immutable RAW layer, pipeline run log, reject record audit trail
- **PCI-DSS** — Masked account numbers, no raw card data

### Business Rules
- Credit scores must be between 300 and 850
- Fraud scores must be between 0.0 and 1.0
- Valid customer statuses: ACTIVE, INACTIVE, SUSPENDED
- Valid account types: CHECKING, SAVINGS, LOAN, CREDIT
- Valid transaction types: DEPOSIT, WITHDRAWAL, TRANSFER, PAYMENT, FEE
- Masked account numbers format: `****XXXX`

---

## Environment Configuration

| Environment | Config File | Usage |
|---|---|---|
| Development | `configs/dev/dev.env` | Local development |
| UAT | `configs/uat/uat.env` | Pre-production testing |
| Production | `configs/prod/prod.env` | Live pipeline |

Never hardcode environment-specific values.
Always use `get_settings()` from `configs/settings.py`.

---

## What Claude Should Never Do

- Never suggest dbt as a replacement for Snowflake SQL transforms
- Never use `print()` — always use `get_logger(__name__)`
- Never commit credentials or secrets to any file
- Never use `SELECT *` in production SQL
- Never write manual retry loops — use `tenacity`
- Never skip `dq_is_valid = TRUE` filter when reading Silver
- Never hardcode bucket names, account identifiers, or passwords
- Never modify Bronze tables after initial load — they are immutable

---

## Running the Pipeline
```bash
# Generate sample data
python data/sample/generate_data.py

# Upload to S3
python ingestion/scripts/run_ingestion.py

# Run data quality checks
python data_quality/expectations/dq_checks.py

# Run tests
pytest tests/ -v
```

---

## Useful Snowflake Queries
```sql
-- Check all layer row counts
SELECT 'RAW' AS layer, 'CUSTOMERS' AS entity, COUNT(*) FROM BANKING_DB.RAW.RAW_CUSTOMERS
UNION ALL SELECT 'SILVER', 'CUSTOMERS', COUNT(*) FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
UNION ALL SELECT 'GOLD', 'DIM_CUSTOMERS', COUNT(*) FROM BANKING_DB.GOLD.DIM_CUSTOMERS;

-- Check DQ pass rates
SELECT
    'SILVER_CUSTOMERS' AS table_name,
    COUNT(*) AS total,
    SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END) AS valid,
    ROUND(SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pass_rate
FROM BANKING_DB.SILVER.SILVER_CUSTOMERS;

-- Latest pipeline batch
SELECT _batch_id, COUNT(*) AS records, MAX(_load_timestamp) AS loaded_at
FROM BANKING_DB.RAW.RAW_TRANSACTIONS
GROUP BY _batch_id
ORDER BY loaded_at DESC
LIMIT 5;
```