-- ============================================================
-- Banking Data Platform — Bronze (RAW) Tables
-- Schema: BANKING_DB.RAW
-- Layer:  Bronze — exact replica of source CSV structure
-- Note:   All columns VARCHAR at this layer — no transforms
--         Data quality happens in Silver layer
-- ============================================================

USE ROLE BANKING_PIPELINE_ROLE;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA RAW;

-- ── Table 1: RAW_CUSTOMERS ────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    -- Source columns (all VARCHAR — raw as-is from CSV)
    customer_id         VARCHAR(50),
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    date_of_birth       VARCHAR(20),
    address_line1       VARCHAR(100),
    address_line2       VARCHAR(100),
    city                VARCHAR(50),
    state               VARCHAR(10),
    zip_code            VARCHAR(10),
    country             VARCHAR(10),
    customer_since      VARCHAR(20),
    status              VARCHAR(20),
    credit_score        VARCHAR(10),
    created_at          VARCHAR(50),
    updated_at          VARCHAR(50),

    -- Pipeline metadata columns
    _source_file        VARCHAR(500)    COMMENT 'S3 source file path',
    _source_entity      VARCHAR(50)     COMMENT 'Source entity name',
    _load_timestamp     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP() COMMENT 'When record was loaded',
    _batch_id           VARCHAR(50)     COMMENT 'Pipeline batch identifier',
    _checksum           VARCHAR(32)     COMMENT 'MD5 checksum of source file'
)
COMMENT = 'Bronze layer — raw customer data from source CSV';

-- ── Table 2: RAW_ACCOUNTS ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW_ACCOUNTS (
    -- Source columns
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    account_number      VARCHAR(20),
    account_type        VARCHAR(20),
    balance             VARCHAR(30),
    available_balance   VARCHAR(30),
    currency            VARCHAR(5),
    interest_rate       VARCHAR(20),
    opened_date         VARCHAR(20),
    status              VARCHAR(20),
    branch_code         VARCHAR(20),
    created_at          VARCHAR(50),
    updated_at          VARCHAR(50),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _source_entity      VARCHAR(50),
    _load_timestamp     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _batch_id           VARCHAR(50),
    _checksum           VARCHAR(32)
)
COMMENT = 'Bronze layer — raw account data from source CSV';

-- ── Table 3: RAW_TRANSACTIONS ─────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW_TRANSACTIONS (
    -- Source columns
    transaction_id      VARCHAR(50),
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    transaction_type    VARCHAR(20),
    amount              VARCHAR(30),
    currency            VARCHAR(5),
    balance_after       VARCHAR(30),
    description         VARCHAR(200),
    merchant_name       VARCHAR(100),
    merchant_category   VARCHAR(50),
    channel             VARCHAR(20),
    status              VARCHAR(20),
    transaction_date    VARCHAR(50),
    value_date          VARCHAR(20),
    reference_number    VARCHAR(50),
    created_at          VARCHAR(50),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _source_entity      VARCHAR(50),
    _load_timestamp     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _batch_id           VARCHAR(50),
    _checksum           VARCHAR(32)
)
COMMENT = 'Bronze layer — raw transaction data from source CSV';

-- ── Table 4: RAW_FRAUD_FLAGS ──────────────────────────────────
CREATE TABLE IF NOT EXISTS RAW_FRAUD_FLAGS (
    -- Source columns
    flag_id             VARCHAR(50),
    transaction_id      VARCHAR(50),
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    flag_reason         VARCHAR(200),
    severity            VARCHAR(20),
    fraud_score         VARCHAR(20),
    is_confirmed_fraud  VARCHAR(10),
    flagged_at          VARCHAR(50),
    reviewed_at         VARCHAR(50),
    reviewed_by         VARCHAR(50),
    created_at          VARCHAR(50),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _source_entity      VARCHAR(50),
    _load_timestamp     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    _batch_id           VARCHAR(50),
    _checksum           VARCHAR(32)
)
COMMENT = 'Bronze layer — raw fraud flag data from source CSV';

-- ── Stage: S3 External Stage ──────────────────────────────────
-- This connects Snowflake directly to our S3 raw bucket
CREATE STAGE IF NOT EXISTS S3_RAW_STAGE
    URL = 's3://banking-data-platform-raw-akshay/raw/'
    FILE_FORMAT = (
        TYPE                = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER         = 1
        NULL_IF             = ('NULL', 'null', 'None', '')
        EMPTY_FIELD_AS_NULL = TRUE
        DATE_FORMAT         = 'AUTO'
        TIMESTAMP_FORMAT    = 'AUTO'
    )
    COMMENT = 'External stage pointing to S3 raw bucket';

-- ── File Format: CSV ──────────────────────────────────────────
CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE                    = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER             = 1
    NULL_IF                 = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL     = TRUE
    DATE_FORMAT             = 'AUTO'
    TIMESTAMP_FORMAT        = 'AUTO'
    COMMENT                 = 'Standard CSV format for banking data';

-- ── Verify Tables Created ─────────────────────────────────────
SHOW TABLES IN SCHEMA BANKING_DB.RAW;