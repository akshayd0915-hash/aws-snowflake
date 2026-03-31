"""
DAG 3: Silver to Gold Transformation
Builds dimensional models (star schema) from Silver layer.

Schedule: Daily at 3 AM (after Silver)
SLA: 3 hours
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from orchestration.dags.dag_config import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE, TAGS_GOLD
)

# ── SQL Statements ────────────────────────────────────────────

TRUNCATE_GOLD = """
TRUNCATE TABLE BANKING_DB.GOLD.FACT_FRAUD;
TRUNCATE TABLE BANKING_DB.GOLD.FACT_TRANSACTIONS;
TRUNCATE TABLE BANKING_DB.GOLD.DIM_ACCOUNTS;
TRUNCATE TABLE BANKING_DB.GOLD.DIM_CUSTOMERS;
"""

LOAD_DIM_CUSTOMERS = """
CREATE OR REPLACE SEQUENCE BANKING_DB.GOLD.SEQ_CUSTOMER_KEY
    START = 1 INCREMENT = 1;
INSERT INTO BANKING_DB.GOLD.DIM_CUSTOMERS (
    customer_key, customer_id, first_name, last_name, full_name,
    email, city, state, country, customer_since, tenure_days,
    tenure_band, age_years, age_band, status, credit_score,
    credit_tier, is_active, _silver_loaded_at, _gold_loaded_at
)
SELECT
    BANKING_DB.GOLD.SEQ_CUSTOMER_KEY.NEXTVAL,
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
    _silver_loaded_at, CURRENT_TIMESTAMP()
FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
WHERE dq_is_valid = TRUE;
"""

LOAD_DIM_ACCOUNTS = """
CREATE OR REPLACE SEQUENCE BANKING_DB.GOLD.SEQ_ACCOUNT_KEY
    START = 1 INCREMENT = 1;
INSERT INTO BANKING_DB.GOLD.DIM_ACCOUNTS (
    account_key, account_id, customer_id, account_number,
    account_type, account_type_desc, currency, interest_rate,
    opened_date, account_age_days, account_age_band,
    status, branch_code, is_overdrawn, is_active,
    _silver_loaded_at, _gold_loaded_at
)
SELECT
    BANKING_DB.GOLD.SEQ_ACCOUNT_KEY.NEXTVAL,
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
    _silver_loaded_at, CURRENT_TIMESTAMP()
FROM BANKING_DB.SILVER.SILVER_ACCOUNTS
WHERE dq_is_valid = TRUE;
"""

LOAD_FACT_TRANSACTIONS = """
CREATE OR REPLACE SEQUENCE BANKING_DB.GOLD.SEQ_TRANSACTION_KEY
    START = 1 INCREMENT = 1;
INSERT INTO BANKING_DB.GOLD.FACT_TRANSACTIONS (
    transaction_key, transaction_id,
    account_key, customer_key, date_key,
    account_id, customer_id, transaction_type,
    amount, amount_abs, is_debit, is_credit, currency,
    balance_after, merchant_name, merchant_category,
    channel, status, transaction_date, value_date,
    reference_number, is_fraud_flagged, _gold_loaded_at
)
WITH fraud_txns AS (
    SELECT DISTINCT transaction_id
    FROM BANKING_DB.SILVER.SILVER_FRAUD_FLAGS
    WHERE dq_is_valid = TRUE
)
SELECT
    BANKING_DB.GOLD.SEQ_TRANSACTION_KEY.NEXTVAL,
    t.transaction_id, a.account_key, c.customer_key, d.date_key,
    t.account_id, t.customer_id, t.transaction_type,
    t.amount, t.amount_abs, t.is_debit, t.is_credit, t.currency,
    t.balance_after, t.merchant_name, t.merchant_category,
    t.channel, t.status, t.transaction_date, t.value_date,
    t.reference_number,
    CASE WHEN f.transaction_id IS NOT NULL THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS t
LEFT JOIN BANKING_DB.GOLD.DIM_ACCOUNTS a ON t.account_id = a.account_id
LEFT JOIN BANKING_DB.GOLD.DIM_CUSTOMERS c ON t.customer_id = c.customer_id
LEFT JOIN BANKING_DB.GOLD.DIM_DATE d ON t.transaction_date_key = d.date_key
LEFT JOIN fraud_txns f ON t.transaction_id = f.transaction_id
WHERE t.dq_is_valid = TRUE;
"""

LOAD_FACT_FRAUD = """
CREATE OR REPLACE SEQUENCE BANKING_DB.GOLD.SEQ_FRAUD_KEY
    START = 1 INCREMENT = 1;
INSERT INTO BANKING_DB.GOLD.FACT_FRAUD (
    fraud_key, flag_id,
    transaction_key, account_key, customer_key, date_key,
    transaction_id, account_id, customer_id,
    flag_reason, severity, severity_rank,
    fraud_score, fraud_score_band, is_confirmed_fraud,
    flagged_at, reviewed_at, days_to_review,
    review_sla_met, _gold_loaded_at
)
SELECT
    BANKING_DB.GOLD.SEQ_FRAUD_KEY.NEXTVAL,
    f.flag_id, t.transaction_key, a.account_key,
    c.customer_key, d.date_key,
    f.transaction_id, f.account_id, f.customer_id,
    f.flag_reason, f.severity, f.severity_rank, f.fraud_score,
    CASE
        WHEN f.fraud_score >= 0.9 THEN 'CRITICAL'
        WHEN f.fraud_score >= 0.7 THEN 'HIGH'
        WHEN f.fraud_score >= 0.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END,
    f.is_confirmed_fraud, f.flagged_at, f.reviewed_at,
    f.days_to_review,
    CASE WHEN f.days_to_review <= 3 THEN TRUE ELSE FALSE END,
    CURRENT_TIMESTAMP()
FROM BANKING_DB.SILVER.SILVER_FRAUD_FLAGS f
LEFT JOIN BANKING_DB.GOLD.FACT_TRANSACTIONS t
    ON f.transaction_id = t.transaction_id
LEFT JOIN BANKING_DB.GOLD.DIM_ACCOUNTS a ON f.account_id = a.account_id
LEFT JOIN BANKING_DB.GOLD.DIM_CUSTOMERS c ON f.customer_id = c.customer_id
LEFT JOIN BANKING_DB.GOLD.DIM_DATE d
    ON TO_NUMBER(TO_VARCHAR(f.flagged_at::DATE, 'YYYYMMDD')) = d.date_key
WHERE f.dq_is_valid = TRUE;
"""

VERIFY_GOLD = """
SELECT 'DIM_CUSTOMERS'     AS table_name, COUNT(*) AS row_count
    FROM BANKING_DB.GOLD.DIM_CUSTOMERS
UNION ALL
SELECT 'DIM_ACCOUNTS',      COUNT(*) FROM BANKING_DB.GOLD.DIM_ACCOUNTS
UNION ALL
SELECT 'DIM_DATE',          COUNT(*) FROM BANKING_DB.GOLD.DIM_DATE
UNION ALL
SELECT 'FACT_TRANSACTIONS', COUNT(*) FROM BANKING_DB.GOLD.FACT_TRANSACTIONS
UNION ALL
SELECT 'FACT_FRAUD',        COUNT(*) FROM BANKING_DB.GOLD.FACT_FRAUD
ORDER BY table_name;
"""

# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id="banking_03_silver_to_gold",
    description="Build Gold dimensional models from Silver layer",
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=TAGS_GOLD,
    doc_md="""
    ## Silver to Gold DAG
    Builds star schema dimensional models from Silver layer.

    ### Tasks
    1. `start` — Pipeline marker
    2. `truncate_gold` — Clear Gold tables
    3. `load_dim_customers` — Build customer dimension
    4. `load_dim_accounts` — Build account dimension
    5. `load_fact_transactions` — Build transaction fact table
    6. `load_fact_fraud` — Build fraud fact table
    7. `verify_gold` — Verify row counts
    8. `end` — Pipeline marker
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    truncate_gold = SnowflakeOperator(
        task_id="truncate_gold",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=TRUNCATE_GOLD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_dim_customers = SnowflakeOperator(
        task_id="load_dim_customers",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_DIM_CUSTOMERS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_dim_accounts = SnowflakeOperator(
        task_id="load_dim_accounts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_DIM_ACCOUNTS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_fact_transactions = SnowflakeOperator(
        task_id="load_fact_transactions",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_FACT_TRANSACTIONS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_fact_fraud = SnowflakeOperator(
        task_id="load_fact_fraud",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_FACT_FRAUD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    verify_gold = SnowflakeOperator(
        task_id="verify_gold_counts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=VERIFY_GOLD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ──────────────────────────────────────────
    start >> truncate_gold
    truncate_gold >> [load_dim_customers, load_dim_accounts]
    [load_dim_customers, load_dim_accounts] >> load_fact_transactions
    load_fact_transactions >> load_fact_fraud
    load_fact_fraud >> verify_gold >> end