-- ============================================================
-- Banking Data Platform - Silver Layer Tables
-- Schema: BANKING_DB.SILVER
-- Layer:  Silver - cleaned, typed, validated data
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA SILVER;

-- ── Table 1: CUSTOMERS ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SILVER_CUSTOMERS (
    -- Business columns (properly typed)
    customer_id         VARCHAR(50)     NOT NULL,
    first_name          VARCHAR(50)     NOT NULL,
    last_name           VARCHAR(50)     NOT NULL,
    full_name           VARCHAR(100)    NOT NULL,
    email               VARCHAR(100)    NOT NULL,
    phone               VARCHAR(20),
    date_of_birth       DATE            NOT NULL,
    age_years           INTEGER,
    address_line1       VARCHAR(100),
    address_line2       VARCHAR(100),
    city                VARCHAR(50),
    state               VARCHAR(2),
    zip_code            VARCHAR(10),
    country             VARCHAR(2)      DEFAULT 'US',
    customer_since      DATE,
    tenure_days         INTEGER,
    status              VARCHAR(20)     NOT NULL,
    credit_score        INTEGER,
    credit_tier         VARCHAR(20),
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,

    -- Data quality flags
    dq_is_valid         BOOLEAN         DEFAULT TRUE,
    dq_issues           VARCHAR(500),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50),
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    -- Constraints
    CONSTRAINT pk_silver_customers PRIMARY KEY (customer_id)
)
COMMENT = 'Silver layer - cleaned and typed customer data';

-- ── Table 2: ACCOUNTS ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS SILVER_ACCOUNTS (
    account_id          VARCHAR(50)     NOT NULL,
    customer_id         VARCHAR(50)     NOT NULL,
    account_number      VARCHAR(20),
    account_type        VARCHAR(20)     NOT NULL,
    balance             NUMBER(18,2),
    available_balance   NUMBER(18,2),
    currency            VARCHAR(3)      DEFAULT 'USD',
    interest_rate       NUMBER(8,4),
    opened_date         DATE,
    account_age_days    INTEGER,
    status              VARCHAR(20),
    branch_code         VARCHAR(20),
    is_overdrawn        BOOLEAN         DEFAULT FALSE,
    created_at          TIMESTAMP_NTZ,
    updated_at          TIMESTAMP_NTZ,

    -- Data quality flags
    dq_is_valid         BOOLEAN         DEFAULT TRUE,
    dq_issues           VARCHAR(500),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50),
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_silver_accounts PRIMARY KEY (account_id)
)
COMMENT = 'Silver layer - cleaned and typed account data';

-- ── Table 3: TRANSACTIONS ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS SILVER_TRANSACTIONS (
    transaction_id      VARCHAR(50)     NOT NULL,
    account_id          VARCHAR(50)     NOT NULL,
    customer_id         VARCHAR(50)     NOT NULL,
    transaction_type    VARCHAR(20)     NOT NULL,
    amount              NUMBER(18,2)    NOT NULL,
    amount_abs          NUMBER(18,2),
    is_debit            BOOLEAN,
    is_credit           BOOLEAN,
    currency            VARCHAR(3)      DEFAULT 'USD',
    balance_after       NUMBER(18,2),
    description         VARCHAR(200),
    merchant_name       VARCHAR(100),
    merchant_category   VARCHAR(50),
    channel             VARCHAR(20),
    status              VARCHAR(20),
    transaction_date    TIMESTAMP_NTZ,
    transaction_date_key INTEGER,
    value_date          DATE,
    reference_number    VARCHAR(50),
    created_at          TIMESTAMP_NTZ,

    -- Data quality flags
    dq_is_valid         BOOLEAN         DEFAULT TRUE,
    dq_issues           VARCHAR(500),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50),
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_silver_transactions PRIMARY KEY (transaction_id)
)
COMMENT = 'Silver layer - cleaned and typed transaction data';

-- ── Table 4: FRAUD FLAGS ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS SILVER_FRAUD_FLAGS (
    flag_id             VARCHAR(50)     NOT NULL,
    transaction_id      VARCHAR(50)     NOT NULL,
    account_id          VARCHAR(50)     NOT NULL,
    customer_id         VARCHAR(50)     NOT NULL,
    flag_reason         VARCHAR(200),
    severity            VARCHAR(20),
    severity_rank       INTEGER,
    fraud_score         NUMBER(8,4),
    is_confirmed_fraud  BOOLEAN         DEFAULT FALSE,
    flagged_at          TIMESTAMP_NTZ,
    reviewed_at         TIMESTAMP_NTZ,
    reviewed_by         VARCHAR(50),
    days_to_review      INTEGER,
    created_at          TIMESTAMP_NTZ,

    -- Data quality flags
    dq_is_valid         BOOLEAN         DEFAULT TRUE,
    dq_issues           VARCHAR(500),

    -- Pipeline metadata
    _source_file        VARCHAR(500),
    _batch_id           VARCHAR(50),
    _silver_loaded_at   TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_silver_fraud_flags PRIMARY KEY (flag_id)
)
COMMENT = 'Silver layer - cleaned and typed fraud flag data';

-- Verify
SHOW TABLES IN SCHEMA BANKING_DB.SILVER;