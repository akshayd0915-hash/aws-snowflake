"""
DAG 2: Bronze to Silver Transformation
Loads data from S3 into Snowflake Bronze tables,
then transforms Bronze → Silver layer.

Schedule: Daily at 1 AM (after ingestion)
SLA: 2 hours
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from orchestration.dags.dag_config import (
    DEFAULT_ARGS, SNOWFLAKE_CONN_ID,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE,
    SNOWFLAKE_ROLE, TAGS_SILVER
)

# ── SQL Statements ────────────────────────────────────────────

LOAD_BRONZE_CUSTOMERS = """
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
COPY INTO BANKING_DB.RAW.RAW_CUSTOMERS (
    customer_id, first_name, last_name, email, phone,
    date_of_birth, address_line1, address_line2, city, state,
    zip_code, country, customer_since, status, credit_score,
    created_at, updated_at, _source_file, _source_entity, _batch_id
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
           $11,$12,$13,$14,$15,$16,$17,
           METADATA$FILENAME, 'customers',
           'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @BANKING_DB.RAW.S3_RAW_STAGE/customers/
)
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"'
               SKIP_HEADER=1 EMPTY_FIELD_AS_NULL=TRUE)
ON_ERROR = 'CONTINUE';
"""

LOAD_BRONZE_ACCOUNTS = """
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
COPY INTO BANKING_DB.RAW.RAW_ACCOUNTS (
    account_id, customer_id, account_number, account_type,
    balance, available_balance, currency, interest_rate,
    opened_date, status, branch_code, created_at, updated_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,
           METADATA$FILENAME, 'accounts',
           'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @BANKING_DB.RAW.S3_RAW_STAGE/accounts/
)
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"'
               SKIP_HEADER=1 EMPTY_FIELD_AS_NULL=TRUE)
ON_ERROR = 'CONTINUE';
"""

LOAD_BRONZE_TRANSACTIONS = """
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
COPY INTO BANKING_DB.RAW.RAW_TRANSACTIONS (
    transaction_id, account_id, customer_id, transaction_type,
    amount, currency, balance_after, description,
    merchant_name, merchant_category, channel, status,
    transaction_date, value_date, reference_number, created_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
           $13,$14,$15,$16,
           METADATA$FILENAME, 'transactions',
           'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @BANKING_DB.RAW.S3_RAW_STAGE/transactions/
)
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"'
               SKIP_HEADER=1 EMPTY_FIELD_AS_NULL=TRUE)
ON_ERROR = 'CONTINUE';
"""

LOAD_BRONZE_FRAUD = """
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
COPY INTO BANKING_DB.RAW.RAW_FRAUD_FLAGS (
    flag_id, transaction_id, account_id, customer_id,
    flag_reason, severity, fraud_score, is_confirmed_fraud,
    flagged_at, reviewed_at, reviewed_by, created_at,
    _source_file, _source_entity, _batch_id
)
FROM (
    SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
           METADATA$FILENAME, 'fraud_flags',
           'BATCH_' || TO_VARCHAR(CURRENT_DATE, 'YYYYMMDD')
    FROM @BANKING_DB.RAW.S3_RAW_STAGE/fraud_flags/
)
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"'
               SKIP_HEADER=1 EMPTY_FIELD_AS_NULL=TRUE)
ON_ERROR = 'CONTINUE';
"""

VERIFY_BRONZE_COUNTS = """
SELECT
    'RAW_CUSTOMERS'    AS table_name, COUNT(*) AS row_count
    FROM BANKING_DB.RAW.RAW_CUSTOMERS
UNION ALL
SELECT 'RAW_ACCOUNTS',     COUNT(*) FROM BANKING_DB.RAW.RAW_ACCOUNTS
UNION ALL
SELECT 'RAW_TRANSACTIONS', COUNT(*) FROM BANKING_DB.RAW.RAW_TRANSACTIONS
UNION ALL
SELECT 'RAW_FRAUD_FLAGS',  COUNT(*) FROM BANKING_DB.RAW.RAW_FRAUD_FLAGS
ORDER BY table_name;
"""

TRUNCATE_SILVER = """
TRUNCATE TABLE BANKING_DB.SILVER.SILVER_CUSTOMERS;
TRUNCATE TABLE BANKING_DB.SILVER.SILVER_ACCOUNTS;
TRUNCATE TABLE BANKING_DB.SILVER.SILVER_TRANSACTIONS;
TRUNCATE TABLE BANKING_DB.SILVER.SILVER_FRAUD_FLAGS;
"""

TRANSFORM_SILVER_CUSTOMERS = """
INSERT INTO BANKING_DB.SILVER.SILVER_CUSTOMERS
WITH deduplicated AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY _load_timestamp DESC
    ) AS rn FROM BANKING_DB.RAW.RAW_CUSTOMERS
),
cleaned AS (
    SELECT
        TRIM(customer_id), INITCAP(TRIM(first_name)),
        INITCAP(TRIM(last_name)),
        INITCAP(TRIM(first_name))||' '||INITCAP(TRIM(last_name)),
        LOWER(TRIM(email)), TRIM(phone),
        TRY_TO_DATE(date_of_birth),
        DATEDIFF('year', TRY_TO_DATE(date_of_birth), CURRENT_DATE()),
        TRIM(address_line1), TRIM(address_line2),
        INITCAP(TRIM(city)), UPPER(TRIM(state)),
        TRIM(zip_code), UPPER(TRIM(country)),
        TRY_TO_DATE(customer_since),
        DATEDIFF('day', TRY_TO_DATE(customer_since), CURRENT_DATE()),
        UPPER(TRIM(status)), TRY_TO_NUMBER(credit_score),
        CASE
            WHEN TRY_TO_NUMBER(credit_score) >= 800 THEN 'EXCEPTIONAL'
            WHEN TRY_TO_NUMBER(credit_score) >= 740 THEN 'VERY_GOOD'
            WHEN TRY_TO_NUMBER(credit_score) >= 670 THEN 'GOOD'
            WHEN TRY_TO_NUMBER(credit_score) >= 580 THEN 'FAIR'
            ELSE 'POOR'
        END,
        TRY_TO_TIMESTAMP_NTZ(created_at),
        TRY_TO_TIMESTAMP_NTZ(updated_at),
        _source_file, _batch_id
    FROM deduplicated WHERE rn = 1
)
SELECT *, TRUE, NULL, _source_file, _batch_id, CURRENT_TIMESTAMP()
FROM cleaned;
"""

VERIFY_SILVER_COUNTS = """
SELECT
    'SILVER_CUSTOMERS' AS table_name,
    COUNT(*) AS row_count,
    SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END) AS valid_count
FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
UNION ALL
SELECT 'SILVER_ACCOUNTS', COUNT(*),
    SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END)
FROM BANKING_DB.SILVER.SILVER_ACCOUNTS
UNION ALL
SELECT 'SILVER_TRANSACTIONS', COUNT(*),
    SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END)
FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS
UNION ALL
SELECT 'SILVER_FRAUD_FLAGS', COUNT(*),
    SUM(CASE WHEN dq_is_valid THEN 1 ELSE 0 END)
FROM BANKING_DB.SILVER.SILVER_FRAUD_FLAGS
ORDER BY table_name;
"""

# ── DAG Definition ────────────────────────────────────────────
with DAG(
    dag_id="banking_02_bronze_to_silver",
    description="Load S3 data into Bronze, transform to Silver",
    default_args=DEFAULT_ARGS,
    schedule="0 1 * * *",
    catchup=False,
    max_active_runs=1,
    tags=TAGS_SILVER,
    doc_md="""
    ## Bronze to Silver DAG
    Loads raw CSVs from S3 into Snowflake Bronze tables,
    then cleans and transforms data into the Silver layer.

    ### Tasks
    1. `start` — Pipeline marker
    2. `load_bronze_*` — COPY INTO Bronze tables from S3 (parallel)
    3. `verify_bronze` — Verify Bronze row counts
    4. `truncate_silver` — Clear Silver for fresh load
    5. `transform_silver_customers` — Bronze → Silver customers
    6. `verify_silver` — Verify Silver row counts and DQ
    7. `end` — Pipeline marker
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # Load Bronze tables in parallel
    load_bronze_customers = SnowflakeOperator(
        task_id="load_bronze_customers",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_BRONZE_CUSTOMERS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_bronze_accounts = SnowflakeOperator(
        task_id="load_bronze_accounts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_BRONZE_ACCOUNTS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_bronze_transactions = SnowflakeOperator(
        task_id="load_bronze_transactions",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_BRONZE_TRANSACTIONS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    load_bronze_fraud = SnowflakeOperator(
        task_id="load_bronze_fraud_flags",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=LOAD_BRONZE_FRAUD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    verify_bronze = SnowflakeOperator(
        task_id="verify_bronze_counts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=VERIFY_BRONZE_COUNTS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    truncate_silver = SnowflakeOperator(
        task_id="truncate_silver",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=TRUNCATE_SILVER,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    transform_customers = SnowflakeOperator(
        task_id="transform_silver_customers",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=TRANSFORM_SILVER_CUSTOMERS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    verify_silver = SnowflakeOperator(
        task_id="verify_silver_counts",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=VERIFY_SILVER_COUNTS,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        role=SNOWFLAKE_ROLE,
    )

    end = EmptyOperator(task_id="end")

    # ── Dependencies ──────────────────────────────────────────
    start >> [
        load_bronze_customers,
        load_bronze_accounts,
        load_bronze_transactions,
        load_bronze_fraud,
    ] >> verify_bronze >> truncate_silver
    truncate_silver >> transform_customers
    transform_customers >> verify_silver >> end