# Environment Configuration Guide

## Overview

This project supports 3 environments — DEV, UAT, PROD.
Each environment has its own parameter file with
environment-specific connections, thresholds, and settings.

## Environment Differences

| Parameter | DEV | UAT | PROD |
|---|---|---|---|
| Log Level | DEBUG | INFO | WARNING |
| Batch Size | 1,000 | 5,000 | 10,000 |
| Max Retries | 3 | 3 | 5 |
| Retry Delay | 30s | 30s | 60s |
| Snowflake DB | BANKING_DB | BANKING_DB_UAT | BANKING_DB_PROD |
| Snowflake WH | BANKING_WH | BANKING_WH_UAT | BANKING_WH_PROD |
| S3 Bucket | *-raw-akshay | *-raw-uat | *-raw-prod |
| Executor | Local | Local | Celery |

## Usage
```bash
# Development (default)
cp configs/dev/dev.env .env

# UAT
cp configs/uat/uat.env .env

# Production
cp configs/prod/prod.env .env
```

## Security Rules

- Never commit actual passwords to any env file
- Use `your_*_password_here` as placeholder
- Real credentials stored in AWS Secrets Manager (prod)
- GitHub Actions secrets for CI/CD pipelines
- Local `.env` file is gitignored — never committed