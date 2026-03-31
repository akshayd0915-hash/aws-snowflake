-- ============================================================
-- Banking Data Platform — Snowflake Infrastructure Setup
-- Account: OXDCMBX-SSC28926
-- Region:  US East (N. Virginia)
-- Author:  Akshay Dubey
-- ============================================================

-- ── Step 1: Set context as SYSADMIN ──────────────────────────
USE ROLE SYSADMIN;

-- ── Step 2: Create Virtual Warehouse ─────────────────────────
CREATE WAREHOUSE IF NOT EXISTS BANKING_WH
    WAREHOUSE_SIZE    = 'X-SMALL'    -- Cost-effective for dev
    AUTO_SUSPEND      = 60           -- Suspend after 60s idle
    AUTO_RESUME       = TRUE         -- Auto-resume on query
    INITIALLY_SUSPENDED = TRUE       -- Start suspended
    COMMENT = 'Primary warehouse for Banking Data Platform';

-- ── Step 3: Create Database ───────────────────────────────────
CREATE DATABASE IF NOT EXISTS BANKING_DB
    COMMENT = 'Banking Data Platform — Medallion Architecture';

-- ── Step 4: Create Schemas (Medallion Architecture) ──────────
-- Bronze: Raw data as-is from S3
CREATE SCHEMA IF NOT EXISTS BANKING_DB.RAW
    COMMENT = 'Bronze layer — raw data landed from S3';

-- Silver: Cleaned, validated, typed data
CREATE SCHEMA IF NOT EXISTS BANKING_DB.SILVER
    COMMENT = 'Silver layer — cleaned and validated data';

-- Gold: Business-level dimensional models
CREATE SCHEMA IF NOT EXISTS BANKING_DB.GOLD
    COMMENT = 'Gold layer — dimensional models for analytics';

-- Audit: Pipeline run tracking
CREATE SCHEMA IF NOT EXISTS BANKING_DB.AUDIT
    COMMENT = 'Pipeline audit logs and run history';

-- ── Step 5: Create Pipeline Role ──────────────────────────────
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS BANKING_PIPELINE_ROLE
    COMMENT = 'Role for data pipeline operations';

CREATE ROLE IF NOT EXISTS BANKING_ANALYST_ROLE
    COMMENT = 'Role for analysts — read only on Gold layer';

-- Grant roles to SYSADMIN (for management)
GRANT ROLE BANKING_PIPELINE_ROLE TO ROLE SYSADMIN;
GRANT ROLE BANKING_ANALYST_ROLE  TO ROLE SYSADMIN;

-- Grant roles to your user
GRANT ROLE BANKING_PIPELINE_ROLE TO USER AKSHAYDUBEY401;
GRANT ROLE BANKING_ANALYST_ROLE  TO USER AKSHAYDUBEY401;

-- ── Step 6: Grant Privileges ──────────────────────────────────
USE ROLE SYSADMIN;

-- Warehouse access
GRANT USAGE ON WAREHOUSE BANKING_WH TO ROLE BANKING_PIPELINE_ROLE;
GRANT USAGE ON WAREHOUSE BANKING_WH TO ROLE BANKING_ANALYST_ROLE;

-- Database access
GRANT USAGE ON DATABASE BANKING_DB TO ROLE BANKING_PIPELINE_ROLE;
GRANT USAGE ON DATABASE BANKING_DB TO ROLE BANKING_ANALYST_ROLE;

-- Pipeline role — full access to all schemas
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE
    ON SCHEMA BANKING_DB.RAW    TO ROLE BANKING_PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE
    ON SCHEMA BANKING_DB.SILVER TO ROLE BANKING_PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW, CREATE STAGE
    ON SCHEMA BANKING_DB.GOLD   TO ROLE BANKING_PIPELINE_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW
    ON SCHEMA BANKING_DB.AUDIT  TO ROLE BANKING_PIPELINE_ROLE;

-- Analyst role — read only on Gold
GRANT USAGE ON SCHEMA BANKING_DB.GOLD TO ROLE BANKING_ANALYST_ROLE;

-- ── Step 7: Create Audit Table ────────────────────────────────
USE ROLE BANKING_PIPELINE_ROLE;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;
USE SCHEMA AUDIT;

CREATE TABLE IF NOT EXISTS PIPELINE_RUN_LOG (
    run_id          VARCHAR(50)     NOT NULL,
    pipeline_name   VARCHAR(100)    NOT NULL,
    entity          VARCHAR(50)     NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    records_read    INTEGER         DEFAULT 0,
    records_loaded  INTEGER         DEFAULT 0,
    records_failed  INTEGER         DEFAULT 0,
    started_at      TIMESTAMP_NTZ   NOT NULL,
    completed_at    TIMESTAMP_NTZ,
    error_message   VARCHAR(5000),
    s3_source_path  VARCHAR(500),
    created_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
);

-- ── Step 8: Verify Setup ──────────────────────────────────────
SHOW WAREHOUSES   LIKE 'BANKING%';
SHOW DATABASES    LIKE 'BANKING%';
SHOW SCHEMAS      IN DATABASE BANKING_DB;
SHOW ROLES        LIKE 'BANKING%';