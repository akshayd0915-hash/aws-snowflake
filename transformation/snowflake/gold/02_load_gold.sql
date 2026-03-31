-- ============================================================
-- Banking Data Platform - Gold Layer Data Load
-- Schema: BANKING_DB.GOLD
-- Pattern: Star Schema
-- Tables: DIM_CUSTOMERS, DIM_ACCOUNTS, DIM_DATE,
--         FACT_TRANSACTIONS, FACT_FRAUD
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;

-- ============================================================
-- STEP 1: CREATE SEQUENCES
-- ============================================================

CREATE OR REPLACE SEQUENCE GOLD.SEQ_CUSTOMER_KEY
    START = 1 INCREMENT = 1;

CREATE OR REPLACE SEQUENCE GOLD.SEQ_ACCOUNT_KEY
    START = 1 INCREMENT = 1;

CREATE OR REPLACE SEQUENCE GOLD.SEQ_TRANSACTION_KEY
    START = 1 INCREMENT = 1;

CREATE OR REPLACE SEQUENCE GOLD.SEQ_FRAUD_KEY
    START = 1 INCREMENT = 1;

-- ============================================================
-- STEP 2: RECREATE TABLES (clean slate)
-- ============================================================

DROP TABLE IF EXISTS GOLD.FACT_FRAUD;
DROP TABLE IF EXISTS GOLD.FACT_TRANSACTIONS;
DROP TABLE IF EXISTS GOLD.DIM_DATE;
DROP TABLE IF EXISTS GOLD.DIM_ACCOUNTS;
DROP TABLE IF EXISTS GOLD.DIM_CUSTOMERS;

-- DIM_CUSTOMERS
CREATE TABLE GOLD.DIM_CUSTOMERS (
    customer_key        INTEGER         NOT NULL PRIMARY KEY,
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
);

-- DIM_ACCOUNTS
CREATE TABLE GOLD.DIM_ACCOUNTS (
    account_key         INTEGER         NOT NULL PRIMARY KEY,
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
);

-- DIM_DATE
CREATE TABLE GOLD.DIM_DATE (
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
);

-- FACT_TRANSACTIONS
CREATE TABLE GOLD.FACT_TRANSACTIONS (
    transaction_key     INTEGER         NOT NULL PRIMARY KEY,
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
);

-- FACT_FRAUD
CREATE TABLE GOLD.FACT_FRAUD (
    fraud_key           INTEGER         NOT NULL PRIMARY KEY,
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
);

-- ============================================================
-- STEP 3: LOAD DIMENSIONS
-- ============================================================

-- DIM_DATE
INSERT INTO GOLD.DIM_DATE
WITH date_spine AS (
    SELECT DATEADD('day', SEQ4(), '2020-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 2922))
)
SELECT DISTINCT
    TO_NUMBER(TO_VARCHAR(full_date, 'YYYYMMDD')),
    full_date,
    DAYOFWEEK(full_date),
    DAYNAME(full_date),
    DAY(full_date),
    DAYOFYEAR(full_date),
    WEEKOFYEAR(full_date),
    MONTH(full_date),
    MONTHNAME(full_date),
    LEFT(MONTHNAME(full_date), 3),
    QUARTER(full_date),
    'Q' || QUARTER(full_date),
    YEAR(full_date),
    CASE WHEN DAYOFWEEK(full_date) IN (0,6) THEN TRUE ELSE FALSE END,
    CASE WHEN DAYOFWEEK(full_date) NOT IN (0,6) THEN TRUE ELSE FALSE END,
    CASE WHEN DAY(full_date) = 1 THEN TRUE ELSE FALSE END,
    CASE WHEN full_date = LAST_DAY(full_date) THEN TRUE ELSE FALSE END,
    CASE WHEN full_date = DATE_TRUNC('quarter', full_date) THEN TRUE ELSE FALSE END,
    CASE WHEN full_date = LAST_DAY(full_date, 'quarter') THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM date_spine;

-- DIM_CUSTOMERS
INSERT INTO GOLD.DIM_CUSTOMERS (
    customer_key, customer_id, first_name, last_name, full_name,
    email, city, state, country, customer_since, tenure_days,
    tenure_band, age_years, age_band, status, credit_score,
    credit_tier, is_active, _silver_loaded_at, _gold_loaded_at
)
SELECT
    GOLD.SEQ_CUSTOMER_KEY.NEXTVAL,
    customer_id, first_name, last_name, full_name,
    email, city, state, country, customer_since, tenure_days,
    CASE
        WHEN tenure_days < 365   THEN '0-1 Years'
        WHEN tenure_days < 730   THEN '1-2 Years'
        WHEN tenure_days < 1825  THEN '2-5 Years'
        WHEN tenure_days < 3650  THEN '5-10 Years'
        ELSE '10+ Years'
    END,
    age_years,
    CASE
        WHEN age_years < 25 THEN '18-24'
        WHEN age_years < 35 THEN '25-34'
        WHEN age_years < 45 THEN '35-44'
        WHEN age_years < 55 THEN '45-54'
        WHEN age_years < 65 THEN '55-64'
        ELSE '65+'
    END,
    status, credit_score, credit_tier,
    CASE WHEN status = 'ACTIVE' THEN TRUE ELSE FALSE END,
    _silver_loaded_at,
    CURRENT_TIMESTAMP()
FROM SILVER.SILVER_CUSTOMERS
WHERE dq_is_valid = TRUE;

-- DIM_ACCOUNTS
INSERT INTO GOLD.DIM_ACCOUNTS (
    account_key, account_id, customer_id, account_number,
    account_type, account_type_desc, currency, interest_rate,
    opened_date, account_age_days, account_age_band,
    status, branch_code, is_overdrawn, is_active,
    _silver_loaded_at, _gold_loaded_at
)
SELECT
    GOLD.SEQ_ACCOUNT_KEY.NEXTVAL,
    account_id, customer_id, account_number, account_type,
    CASE account_type
        WHEN 'CHECKING' THEN 'Checking Account'
        WHEN 'SAVINGS'  THEN 'Savings Account'
        WHEN 'LOAN'     THEN 'Loan Account'
        WHEN 'CREDIT'   THEN 'Credit Card Account'
        ELSE 'Unknown'
    END,
    currency, interest_rate, opened_date, account_age_days,
    CASE
        WHEN account_age_days < 365   THEN '0-1 Years'
        WHEN account_age_days < 730   THEN '1-2 Years'
        WHEN account_age_days < 1825  THEN '2-5 Years'
        WHEN account_age_days < 3650  THEN '5-10 Years'
        ELSE '10+ Years'
    END,
    status, branch_code, is_overdrawn,
    CASE WHEN status = 'ACTIVE' THEN TRUE ELSE FALSE END,
    _silver_loaded_at,
    CURRENT_TIMESTAMP()
FROM SILVER.SILVER_ACCOUNTS
WHERE dq_is_valid = TRUE;

-- ============================================================
-- STEP 4: LOAD FACTS
-- ============================================================

-- FACT_TRANSACTIONS
INSERT INTO GOLD.FACT_TRANSACTIONS (
    transaction_key, transaction_id,
    account_key, customer_key, date_key,
    account_id, customer_id,
    transaction_type, amount, amount_abs,
    is_debit, is_credit, currency,
    balance_after, merchant_name, merchant_category,
    channel, status, transaction_date,
    value_date, reference_number,
    is_fraud_flagged, _gold_loaded_at
)
WITH fraud_txns AS (
    SELECT DISTINCT transaction_id
    FROM SILVER.SILVER_FRAUD_FLAGS
    WHERE dq_is_valid = TRUE
)
SELECT
    GOLD.SEQ_TRANSACTION_KEY.NEXTVAL,
    t.transaction_id,
    a.account_key,
    c.customer_key,
    d.date_key,
    t.account_id,
    t.customer_id,
    t.transaction_type,
    t.amount,
    t.amount_abs,
    t.is_debit,
    t.is_credit,
    t.currency,
    t.balance_after,
    t.merchant_name,
    t.merchant_category,
    t.channel,
    t.status,
    t.transaction_date,
    t.value_date,
    t.reference_number,
    CASE WHEN f.transaction_id IS NOT NULL THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM SILVER.SILVER_TRANSACTIONS t
LEFT JOIN GOLD.DIM_ACCOUNTS a ON t.account_id = a.account_id
LEFT JOIN GOLD.DIM_CUSTOMERS c ON t.customer_id = c.customer_id
LEFT JOIN GOLD.DIM_DATE d ON t.transaction_date_key = d.date_key
LEFT JOIN fraud_txns f ON t.transaction_id = f.transaction_id
WHERE t.dq_is_valid = TRUE;

-- FACT_FRAUD
INSERT INTO GOLD.FACT_FRAUD (
    fraud_key, flag_id,
    transaction_key, account_key, customer_key, date_key,
    transaction_id, account_id, customer_id,
    flag_reason, severity, severity_rank,
    fraud_score, fraud_score_band,
    is_confirmed_fraud, flagged_at, reviewed_at,
    days_to_review, review_sla_met, _gold_loaded_at
)
SELECT
    GOLD.SEQ_FRAUD_KEY.NEXTVAL,
    f.flag_id,
    t.transaction_key,
    a.account_key,
    c.customer_key,
    d.date_key,
    f.transaction_id,
    f.account_id,
    f.customer_id,
    f.flag_reason,
    f.severity,
    f.severity_rank,
    f.fraud_score,
    CASE
        WHEN f.fraud_score >= 0.9 THEN 'CRITICAL'
        WHEN f.fraud_score >= 0.7 THEN 'HIGH'
        WHEN f.fraud_score >= 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    f.is_confirmed_fraud,
    f.flagged_at,
    f.reviewed_at,
    f.days_to_review,
    CASE WHEN f.days_to_review <= 3 THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM SILVER.SILVER_FRAUD_FLAGS f
LEFT JOIN GOLD.FACT_TRANSACTIONS t ON f.transaction_id = t.transaction_id
LEFT JOIN GOLD.DIM_ACCOUNTS a ON f.account_id = a.account_id
LEFT JOIN GOLD.DIM_CUSTOMERS c ON f.customer_id = c.customer_id
LEFT JOIN GOLD.DIM_DATE d
    ON TO_NUMBER(TO_VARCHAR(f.flagged_at::DATE, 'YYYYMMDD')) = d.date_key
WHERE f.dq_is_valid = TRUE;

-- ============================================================
-- STEP 5: VERIFY
-- ============================================================

SELECT 'DIM_CUSTOMERS'    AS table_name, COUNT(*) AS row_count FROM GOLD.DIM_CUSTOMERS
UNION ALL
SELECT 'DIM_ACCOUNTS',     COUNT(*) FROM GOLD.DIM_ACCOUNTS
UNION ALL
SELECT 'DIM_DATE',         COUNT(*) FROM GOLD.DIM_DATE
UNION ALL
SELECT 'FACT_TRANSACTIONS', COUNT(*) FROM GOLD.FACT_TRANSACTIONS
UNION ALL
SELECT 'FACT_FRAUD',       COUNT(*) FROM GOLD.FACT_FRAUD
ORDER BY table_name;