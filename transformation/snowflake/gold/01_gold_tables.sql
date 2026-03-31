-- ============================================================
-- Banking Data Platform - Gold Layer Tables
-- Schema: BANKING_DB.GOLD
-- Layer:  Gold - dimensional models for analytics
-- Pattern: Star Schema
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA GOLD;

-- ── Dimension 1: DIM_CUSTOMERS ────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_CUSTOMERS (
    customer_key        INTEGER         AUTOINCREMENT PRIMARY KEY,
    customer_id         VARCHAR(50)     NOT NULL UNIQUE,
    first_name          VARCHAR(50),
    last_name           VARCHAR(50),
    full_name           VARCHAR(100),
    email               VARCHAR(100),
    city                VARCHAR(50),
    state               VARCHAR(2),
    country             VARCHAR(2),
    customer_since      DATE,
    tenure_days         INTEGER,
    tenure_band         VARCHAR(20),
    age_years           INTEGER,
    age_band            VARCHAR(20),
    status              VARCHAR(20),
    credit_score        INTEGER,
    credit_tier         VARCHAR(20),
    is_active           BOOLEAN         DEFAULT TRUE,
    _silver_loaded_at   TIMESTAMP_NTZ,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer - customer dimension';

-- ── Dimension 2: DIM_ACCOUNTS ─────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_ACCOUNTS (
    account_key         INTEGER         AUTOINCREMENT PRIMARY KEY,
    account_id          VARCHAR(50)     NOT NULL UNIQUE,
    customer_id         VARCHAR(50)     NOT NULL,
    account_number      VARCHAR(20),
    account_type        VARCHAR(20),
    account_type_desc   VARCHAR(50),
    currency            VARCHAR(3),
    interest_rate       NUMBER(8,4),
    opened_date         DATE,
    account_age_days    INTEGER,
    account_age_band    VARCHAR(20),
    status              VARCHAR(20),
    branch_code         VARCHAR(20),
    is_overdrawn        BOOLEAN,
    is_active           BOOLEAN         DEFAULT TRUE,
    _silver_loaded_at   TIMESTAMP_NTZ,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer - account dimension';

-- ── Dimension 3: DIM_DATE ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS DIM_DATE (
    date_key            INTEGER         PRIMARY KEY,
    full_date           DATE            NOT NULL,
    day_of_week         INTEGER,
    day_name            VARCHAR(10),
    day_of_month        INTEGER,
    day_of_year         INTEGER,
    week_of_year        INTEGER,
    month_number        INTEGER,
    month_name          VARCHAR(10),
    month_short         VARCHAR(3),
    quarter_number      INTEGER,
    quarter_name        VARCHAR(6),
    year_number         INTEGER,
    is_weekend          BOOLEAN,
    is_weekday          BOOLEAN,
    is_month_start      BOOLEAN,
    is_month_end        BOOLEAN,
    is_quarter_start    BOOLEAN,
    is_quarter_end      BOOLEAN,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer - date dimension';

-- ── Fact Table 1: FACT_TRANSACTIONS ───────────────────────────
CREATE TABLE IF NOT EXISTS FACT_TRANSACTIONS (
    transaction_key     INTEGER         AUTOINCREMENT PRIMARY KEY,
    transaction_id      VARCHAR(50)     NOT NULL UNIQUE,
    account_key         INTEGER,
    customer_key        INTEGER,
    date_key            INTEGER,
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    transaction_type    VARCHAR(20),
    amount              NUMBER(18,2),
    amount_abs          NUMBER(18,2),
    is_debit            BOOLEAN,
    is_credit           BOOLEAN,
    currency            VARCHAR(3),
    balance_after       NUMBER(18,2),
    merchant_name       VARCHAR(100),
    merchant_category   VARCHAR(50),
    channel             VARCHAR(20),
    status              VARCHAR(20),
    transaction_date    TIMESTAMP_NTZ,
    value_date          DATE,
    reference_number    VARCHAR(50),
    is_fraud_flagged    BOOLEAN         DEFAULT FALSE,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer - transaction fact table';

-- ── Fact Table 2: FACT_FRAUD ──────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_FRAUD (
    fraud_key           INTEGER         AUTOINCREMENT PRIMARY KEY,
    flag_id             VARCHAR(50)     NOT NULL UNIQUE,
    transaction_key     INTEGER,
    account_key         INTEGER,
    customer_key        INTEGER,
    date_key            INTEGER,
    transaction_id      VARCHAR(50),
    account_id          VARCHAR(50),
    customer_id         VARCHAR(50),
    flag_reason         VARCHAR(200),
    severity            VARCHAR(20),
    severity_rank       INTEGER,
    fraud_score         NUMBER(8,4),
    fraud_score_band    VARCHAR(20),
    is_confirmed_fraud  BOOLEAN,
    flagged_at          TIMESTAMP_NTZ,
    reviewed_at         TIMESTAMP_NTZ,
    days_to_review      INTEGER,
    review_sla_met      BOOLEAN,
    _gold_loaded_at     TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Gold layer - fraud analytics fact table';

-- Verify
SHOW TABLES IN SCHEMA BANKING_DB.GOLD;