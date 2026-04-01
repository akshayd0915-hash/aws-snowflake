-- ============================================================
-- Banking Data Platform - Reject Record Routing
-- Routes failed DQ records to ERROR.REJECT_RECORDS
-- Satisfies SOX audit requirement — no silent data drops
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;

-- ── Insert failed Silver customers into ERROR table ───────────
INSERT INTO ERROR.REJECT_RECORDS (
    reject_id, source_table, source_layer,
    batch_id, entity, reject_reason,
    reject_rule, severity,
    customer_id, rejected_at, pipeline_name
)
SELECT
    'REJ_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')
             || '_' || customer_id,
    'SILVER_CUSTOMERS',
    'SILVER',
    _batch_id,
    'customers',
    COALESCE(dq_issues, 'Unknown DQ failure'),
    'DQ_SILVER_CUSTOMERS',
    'HIGH',
    customer_id,
    CURRENT_TIMESTAMP(),
    'bronze_to_silver_dag'
FROM BANKING_DB.SILVER.SILVER_CUSTOMERS
WHERE dq_is_valid = FALSE;

-- ── Insert failed Silver transactions into ERROR table ────────
INSERT INTO ERROR.REJECT_RECORDS (
    reject_id, source_table, source_layer,
    batch_id, entity, reject_reason,
    reject_rule, severity,
    account_id, transaction_id,
    customer_id, rejected_at, pipeline_name
)
SELECT
    'REJ_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')
             || '_' || transaction_id,
    'SILVER_TRANSACTIONS',
    'SILVER',
    _batch_id,
    'transactions',
    COALESCE(dq_issues, 'Unknown DQ failure'),
    'DQ_SILVER_TRANSACTIONS',
    'CRITICAL',
    account_id,
    transaction_id,
    customer_id,
    CURRENT_TIMESTAMP(),
    'bronze_to_silver_dag'
FROM BANKING_DB.SILVER.SILVER_TRANSACTIONS
WHERE dq_is_valid = FALSE;

-- ── Insert failed Silver accounts into ERROR table ────────────
INSERT INTO ERROR.REJECT_RECORDS (
    reject_id, source_table, source_layer,
    batch_id, entity, reject_reason,
    reject_rule, severity,
    account_id, customer_id,
    rejected_at, pipeline_name
)
SELECT
    'REJ_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')
             || '_' || account_id,
    'SILVER_ACCOUNTS',
    'SILVER',
    _batch_id,
    'accounts',
    COALESCE(dq_issues, 'Unknown DQ failure'),
    'DQ_SILVER_ACCOUNTS',
    'HIGH',
    account_id,
    customer_id,
    CURRENT_TIMESTAMP(),
    'bronze_to_silver_dag'
FROM BANKING_DB.SILVER.SILVER_ACCOUNTS
WHERE dq_is_valid = FALSE;

-- ── Insert failed Silver fraud flags into ERROR table ─────────
INSERT INTO ERROR.REJECT_RECORDS (
    reject_id, source_table, source_layer,
    batch_id, entity, reject_reason,
    reject_rule, severity,
    transaction_id, account_id,
    customer_id, rejected_at, pipeline_name
)
SELECT
    'REJ_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')
             || '_' || flag_id,
    'SILVER_FRAUD_FLAGS',
    'SILVER',
    _batch_id,
    'fraud_flags',
    COALESCE(dq_issues, 'Unknown DQ failure'),
    'DQ_SILVER_FRAUD_FLAGS',
    'CRITICAL',
    transaction_id,
    account_id,
    customer_id,
    CURRENT_TIMESTAMP(),
    'bronze_to_silver_dag'
FROM BANKING_DB.SILVER.SILVER_FRAUD_FLAGS
WHERE dq_is_valid = FALSE;

-- ── Verify reject counts ──────────────────────────────────────
SELECT
    source_table,
    severity,
    COUNT(*) AS reject_count,
    MIN(rejected_at) AS first_rejection,
    MAX(rejected_at) AS last_rejection
FROM ERROR.REJECT_RECORDS
GROUP BY source_table, severity
ORDER BY source_table;

-- ── View all rejects ──────────────────────────────────────────
SELECT
    reject_id,
    source_table,
    source_layer,
    entity,
    reject_reason,
    severity,
    customer_id,
    transaction_id,
    rejected_at
FROM ERROR.REJECT_RECORDS
ORDER BY rejected_at DESC
LIMIT 20;