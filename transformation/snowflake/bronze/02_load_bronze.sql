-- ============================================================
-- Banking Data Platform - Bronze Layer Data Load
-- Loads CSV files from S3 into RAW Snowflake tables
-- Run: Daily batch or triggered via Airflow
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA RAW;

-- Load Customers
COPY INTO RAW_CUSTOMERS (
    customer_id, first_name, last_name, email, phone,
    date_of_birth, address_line1, address_line2, city, state,
    zip_code, country, customer_since, status, credit_score,
    created_at, updated_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT
        $1, $2, $3, $4, $5,
        $6, $7, $8, $9, $10,
        $11, $12, $13, $14, $15,
        $16, $17,
        METADATA$FILENAME,
        'customers',
        'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @S3_RAW_STAGE/customers/
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Load Accounts
COPY INTO RAW_ACCOUNTS (
    account_id, customer_id, account_number, account_type,
    balance, available_balance, currency, interest_rate,
    opened_date, status, branch_code, created_at, updated_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT
        $1, $2, $3, $4,
        $5, $6, $7, $8,
        $9, $10, $11, $12, $13,
        METADATA$FILENAME,
        'accounts',
        'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @S3_RAW_STAGE/accounts/
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Load Transactions
COPY INTO RAW_TRANSACTIONS (
    transaction_id, account_id, customer_id, transaction_type,
    amount, currency, balance_after, description,
    merchant_name, merchant_category, channel, status,
    transaction_date, value_date, reference_number, created_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT
        $1, $2, $3, $4,
        $5, $6, $7, $8,
        $9, $10, $11, $12,
        $13, $14, $15, $16,
        METADATA$FILENAME,
        'transactions',
        'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @S3_RAW_STAGE/transactions/
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Load Fraud Flags
COPY INTO RAW_FRAUD_FLAGS (
    flag_id, transaction_id, account_id, customer_id,
    flag_reason, severity, fraud_score, is_confirmed_fraud,
    flagged_at, reviewed_at, reviewed_by, created_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT
        $1, $2, $3, $4,
        $5, $6, $7, $8,
        $9, $10, $11, $12,
        METADATA$FILENAME,
        'fraud_flags',
        'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @S3_RAW_STAGE/fraud_flags/
)
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Verify Row Counts
SELECT 'RAW_CUSTOMERS'    AS table_name, COUNT(*) AS row_count FROM RAW_CUSTOMERS
UNION ALL
SELECT 'RAW_ACCOUNTS',     COUNT(*) FROM RAW_ACCOUNTS
UNION ALL
SELECT 'RAW_TRANSACTIONS', COUNT(*) FROM RAW_TRANSACTIONS
UNION ALL
SELECT 'RAW_FRAUD_FLAGS',  COUNT(*) FROM RAW_FRAUD_FLAGS
ORDER BY table_name;