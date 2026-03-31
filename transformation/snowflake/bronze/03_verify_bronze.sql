-- ============================================================
-- Banking Data Platform - Bronze Layer Verification Queries
-- Run after each load to verify data quality at Bronze layer
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA RAW;

-- Row counts per table
SELECT 'RAW_CUSTOMERS'    AS table_name, COUNT(*) AS row_count FROM RAW_CUSTOMERS
UNION ALL
SELECT 'RAW_ACCOUNTS',     COUNT(*) FROM RAW_ACCOUNTS
UNION ALL
SELECT 'RAW_TRANSACTIONS', COUNT(*) FROM RAW_TRANSACTIONS
UNION ALL
SELECT 'RAW_FRAUD_FLAGS',  COUNT(*) FROM RAW_FRAUD_FLAGS
ORDER BY table_name;

-- Check latest batch loaded
SELECT
    _batch_id,
    _source_entity,
    COUNT(*) AS records_loaded,
    MIN(_load_timestamp) AS load_started,
    MAX(_load_timestamp) AS load_completed
FROM RAW_CUSTOMERS
GROUP BY _batch_id, _source_entity
ORDER BY load_started DESC;

-- Check for nulls in critical columns
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails,
    SUM(CASE WHEN status IS NULL THEN 1 ELSE 0 END) AS null_statuses
FROM RAW_CUSTOMERS;

-- Sample records
SELECT * FROM RAW_CUSTOMERS LIMIT 5;
SELECT * FROM RAW_TRANSACTIONS LIMIT 5;