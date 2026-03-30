# 🏦 Banking Data Platform

An enterprise-grade data engineering platform built on AWS, Snowflake, and Apache Airflow — implementing a full Medallion Architecture (Bronze → Silver → Gold) for banking analytics.

## 📐 Architecture Overview
```
Source Systems → S3 (Bronze) → Snowflake Silver → Snowflake Gold → Analytics
```

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Cloud | AWS (S3, Glue, Lambda, MWAA) |
| Data Warehouse | Snowflake |
| Orchestration | Apache Airflow (MWAA) |
| Language | Python 3.11, SQL |
| IaC | Terraform |
| CI/CD | GitHub Actions |
| Data Quality | Great Expectations |

## 📁 Project Structure
```
banking-data-platform/
├── ingestion/          # Source ingestion scripts & schemas
├── transformation/     # Snowflake Bronze/Silver/Gold SQL & ELT
├── orchestration/      # Airflow DAGs & plugins
├── data_quality/       # Great Expectations suites & checkpoints
├── infrastructure/     # Terraform IaC & IAM policies
├── ci_cd/              # GitHub Actions workflows
├── configs/            # Environment-specific configs
├── tests/              # Unit & integration tests
└── docs/               # Architecture docs & data contracts
```

## 🏗️ Medallion Architecture

- **Bronze** — Raw data landed from source systems into S3, loaded as-is into Snowflake
- **Silver** — Cleaned, validated, deduplicated, typed data
- **Gold** — Business-level dimensional models for analytics & reporting

## 🚀 Getting Started

See `docs/setup.md` for full setup instructions.

## 📊 Domain

Simulated retail banking data including:
- Customer accounts
- Transactions (deposits, withdrawals, transfers)
- Fraud detection flags
- Regulatory reporting