-- ============================================================
-- Banking Data Platform - Bronze to Silver Transformation
-- Cleans, types, validates and enriches raw Bronze data
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;

-- ── Transform Customers ───────────────────────────────────────
INSERT INTO SILVER.SILVER_CUSTOMERS
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY _load_timestamp DESC
        ) AS rn
    FROM RAW.RAW_CUSTOMERS
),
cleaned AS (
    SELECT
        TRIM(customer_id)                           AS customer_id,
        INITCAP(TRIM(first_name))                   AS first_name,
        INITCAP(TRIM(last_name))                    AS last_name,
        INITCAP(TRIM(first_name)) || ' ' ||
        INITCAP(TRIM(last_name))                    AS full_name,
        LOWER(TRIM(email))                          AS email,
        TRIM(phone)                                 AS phone,
        TRY_TO_DATE(date_of_birth)                  AS date_of_birth,
        DATEDIFF('year',
            TRY_TO_DATE(date_of_birth),
            CURRENT_DATE())                         AS age_years,
        TRIM(address_line1)                         AS address_line1,
        TRIM(address_line2)                         AS address_line2,
        INITCAP(TRIM(city))                         AS city,
        UPPER(TRIM(state))                          AS state,
        TRIM(zip_code)                              AS zip_code,
        UPPER(TRIM(country))                        AS country,
        TRY_TO_DATE(customer_since)                 AS customer_since,
        DATEDIFF('day',
            TRY_TO_DATE(customer_since),
            CURRENT_DATE())                         AS tenure_days,
        UPPER(TRIM(status))                         AS status,
        TRY_TO_NUMBER(credit_score)                 AS credit_score,
        CASE
            WHEN TRY_TO_NUMBER(credit_score) >= 800 THEN 'EXCEPTIONAL'
            WHEN TRY_TO_NUMBER(credit_score) >= 740 THEN 'VERY_GOOD'
            WHEN TRY_TO_NUMBER(credit_score) >= 670 THEN 'GOOD'
            WHEN TRY_TO_NUMBER(credit_score) >= 580 THEN 'FAIR'
            ELSE 'POOR'
        END                                         AS credit_tier,
        TRY_TO_TIMESTAMP_NTZ(created_at)            AS created_at,
        TRY_TO_TIMESTAMP_NTZ(updated_at)            AS updated_at,
        _source_file,
        _batch_id
    FROM deduplicated
    WHERE rn = 1
),
validated AS (
    SELECT *,
        CASE
            WHEN customer_id IS NULL THEN FALSE
            WHEN email IS NULL THEN FALSE
            WHEN date_of_birth IS NULL THEN FALSE
            WHEN status IS NULL THEN FALSE
            ELSE TRUE
        END AS dq_is_valid,
        CASE
            WHEN customer_id IS NULL THEN 'Missing customer_id'
            WHEN email IS NULL THEN 'Missing or invalid email'
            WHEN date_of_birth IS NULL THEN 'Missing or invalid date_of_birth'
            WHEN status IS NULL THEN 'Missing status'
            ELSE NULL
        END AS dq_issues
    FROM cleaned
)
SELECT
    customer_id, first_name, last_name, full_name,
    email, phone, date_of_birth, age_years,
    address_line1, address_line2, city, state,
    zip_code, country, customer_since, tenure_days,
    status, credit_score, credit_tier,
    created_at, updated_at,
    dq_is_valid, dq_issues,
    _source_file, _batch_id,
    CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM validated;

-- ── Transform Accounts ────────────────────────────────────────
INSERT INTO SILVER.SILVER_ACCOUNTS
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY _load_timestamp DESC
        ) AS rn
    FROM RAW.RAW_ACCOUNTS
),
cleaned AS (
    SELECT
        TRIM(account_id)                            AS account_id,
        TRIM(customer_id)                           AS customer_id,
        TRIM(account_number)                        AS account_number,
        UPPER(TRIM(account_type))                   AS account_type,
        TRY_TO_NUMBER(balance, 18, 2)               AS balance,
        TRY_TO_NUMBER(available_balance, 18, 2)     AS available_balance,
        UPPER(TRIM(currency))                       AS currency,
        TRY_TO_NUMBER(interest_rate, 8, 4)          AS interest_rate,
        TRY_TO_DATE(opened_date)                    AS opened_date,
        DATEDIFF('day',
            TRY_TO_DATE(opened_date),
            CURRENT_DATE())                         AS account_age_days,
        UPPER(TRIM(status))                         AS status,
        TRIM(branch_code)                           AS branch_code,
        CASE
            WHEN TRY_TO_NUMBER(balance, 18, 2) < 0
            THEN TRUE ELSE FALSE
        END                                         AS is_overdrawn,
        TRY_TO_TIMESTAMP_NTZ(created_at)            AS created_at,
        TRY_TO_TIMESTAMP_NTZ(updated_at)            AS updated_at,
        _source_file,
        _batch_id
    FROM deduplicated
    WHERE rn = 1
),
validated AS (
    SELECT *,
        CASE
            WHEN account_id IS NULL THEN FALSE
            WHEN customer_id IS NULL THEN FALSE
            WHEN account_type IS NULL THEN FALSE
            ELSE TRUE
        END AS dq_is_valid,
        CASE
            WHEN account_id IS NULL THEN 'Missing account_id'
            WHEN customer_id IS NULL THEN 'Missing customer_id'
            WHEN account_type IS NULL THEN 'Missing account_type'
            ELSE NULL
        END AS dq_issues
    FROM cleaned
)
SELECT
    account_id, customer_id, account_number,
    account_type, balance, available_balance,
    currency, interest_rate, opened_date,
    account_age_days, status, branch_code,
    is_overdrawn, created_at, updated_at,
    dq_is_valid, dq_issues,
    _source_file, _batch_id,
    CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM validated;

-- ── Transform Transactions ────────────────────────────────────
INSERT INTO SILVER.SILVER_TRANSACTIONS
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY _load_timestamp DESC
        ) AS rn
    FROM RAW.RAW_TRANSACTIONS
),
cleaned AS (
    SELECT
        TRIM(transaction_id)                        AS transaction_id,
        TRIM(account_id)                            AS account_id,
        TRIM(customer_id)                           AS customer_id,
        UPPER(TRIM(transaction_type))               AS transaction_type,
        TRY_TO_NUMBER(amount, 18, 2)                AS amount,
        ABS(TRY_TO_NUMBER(amount, 18, 2))           AS amount_abs,
        CASE
            WHEN TRY_TO_NUMBER(amount, 18, 2) < 0
            THEN TRUE ELSE FALSE
        END                                         AS is_debit,
        CASE
            WHEN TRY_TO_NUMBER(amount, 18, 2) > 0
            THEN TRUE ELSE FALSE
        END                                         AS is_credit,
        UPPER(TRIM(currency))                       AS currency,
        TRY_TO_NUMBER(balance_after, 18, 2)         AS balance_after,
        TRIM(description)                           AS description,
        INITCAP(TRIM(merchant_name))                AS merchant_name,
        UPPER(TRIM(merchant_category))              AS merchant_category,
        UPPER(TRIM(channel))                        AS channel,
        UPPER(TRIM(status))                         AS status,
        TRY_TO_TIMESTAMP_NTZ(transaction_date)      AS transaction_date,
        TO_NUMBER(TO_VARCHAR(
            TRY_TO_DATE(transaction_date), 'YYYYMMDD'
        ))                                          AS transaction_date_key,
        TRY_TO_DATE(value_date)                     AS value_date,
        TRIM(reference_number)                      AS reference_number,
        TRY_TO_TIMESTAMP_NTZ(created_at)            AS created_at,
        _source_file,
        _batch_id
    FROM deduplicated
    WHERE rn = 1
),
validated AS (
    SELECT *,
        CASE
            WHEN transaction_id IS NULL THEN FALSE
            WHEN account_id IS NULL THEN FALSE
            WHEN amount IS NULL THEN FALSE
            WHEN transaction_date IS NULL THEN FALSE
            ELSE TRUE
        END AS dq_is_valid,
        CASE
            WHEN transaction_id IS NULL THEN 'Missing transaction_id'
            WHEN account_id IS NULL THEN 'Missing account_id'
            WHEN amount IS NULL THEN 'Missing or invalid amount'
            WHEN transaction_date IS NULL THEN 'Missing or invalid transaction_date'
            ELSE NULL
        END AS dq_issues
    FROM cleaned
)
SELECT
    transaction_id, account_id, customer_id,
    transaction_type, amount, amount_abs,
    is_debit, is_credit, currency,
    balance_after, description, merchant_name,
    merchant_category, channel, status,
    transaction_date, transaction_date_key,
    value_date, reference_number, created_at,
    dq_is_valid, dq_issues,
    _source_file, _batch_id,
    CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM validated;

-- ── Transform Fraud Flags ─────────────────────────────────────
INSERT INTO SILVER.SILVER_FRAUD_FLAGS
WITH deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY flag_id
            ORDER BY _load_timestamp DESC
        ) AS rn
    FROM RAW.RAW_FRAUD_FLAGS
),
cleaned AS (
    SELECT
        TRIM(flag_id)                               AS flag_id,
        TRIM(transaction_id)                        AS transaction_id,
        TRIM(account_id)                            AS account_id,
        TRIM(customer_id)                           AS customer_id,
        TRIM(flag_reason)                           AS flag_reason,
        UPPER(TRIM(severity))                       AS severity,
        CASE UPPER(TRIM(severity))
            WHEN 'CRITICAL' THEN 4
            WHEN 'HIGH'     THEN 3
            WHEN 'MEDIUM'   THEN 2
            WHEN 'LOW'      THEN 1
            ELSE 0
        END                                         AS severity_rank,
        TRY_TO_NUMBER(fraud_score, 8, 4)            AS fraud_score,
        CASE UPPER(TRIM(is_confirmed_fraud))
            WHEN 'TRUE'  THEN TRUE
            WHEN 'FALSE' THEN FALSE
            ELSE FALSE
        END                                         AS is_confirmed_fraud,
        TRY_TO_TIMESTAMP_NTZ(flagged_at)            AS flagged_at,
        TRY_TO_TIMESTAMP_NTZ(reviewed_at)           AS reviewed_at,
        TRIM(reviewed_by)                           AS reviewed_by,
        DATEDIFF('day',
            TRY_TO_TIMESTAMP_NTZ(flagged_at),
            COALESCE(
                TRY_TO_TIMESTAMP_NTZ(reviewed_at),
                CURRENT_TIMESTAMP()
            )
        )                                           AS days_to_review,
        TRY_TO_TIMESTAMP_NTZ(created_at)            AS created_at,
        _source_file,
        _batch_id
    FROM deduplicated
    WHERE rn = 1
),
validated AS (
    SELECT *,
        CASE
            WHEN flag_id IS NULL THEN FALSE
            WHEN transaction_id IS NULL THEN FALSE
            WHEN fraud_score IS NULL THEN FALSE
            ELSE TRUE
        END AS dq_is_valid,
        CASE
            WHEN flag_id IS NULL THEN 'Missing flag_id'
            WHEN transaction_id IS NULL THEN 'Missing transaction_id'
            WHEN fraud_score IS NULL THEN 'Missing or invalid fraud_score'
            ELSE NULL
        END AS dq_issues
    FROM cleaned
)
SELECT
    flag_id, transaction_id, account_id, customer_id,
    flag_reason, severity, severity_rank,
    fraud_score, is_confirmed_fraud,
    flagged_at, reviewed_at, reviewed_by,
    days_to_review, created_at,
    dq_is_valid, dq_issues,
    _source_file, _batch_id,
    CURRENT_TIMESTAMP() AS _silver_loaded_at
FROM validated;

-- ── Verify Silver Layer ───────────────────────────────────────
SELECT 'SILVER_CUSTOMERS'    AS table_name, COUNT(*) AS row_count FROM SILVER.SILVER_CUSTOMERS
UNION ALL
SELECT 'SILVER_ACCOUNTS',     COUNT(*) FROM SILVER.SILVER_ACCOUNTS
UNION ALL
SELECT 'SILVER_TRANSACTIONS', COUNT(*) FROM SILVER.SILVER_TRANSACTIONS
UNION ALL
SELECT 'SILVER_FRAUD_FLAGS',  COUNT(*) FROM SILVER.SILVER_FRAUD_FLAGS
ORDER BY table_name;