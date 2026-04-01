-- ============================================================
-- Banking Data Platform - SCD Type 2 on DIM_CUSTOMERS
-- Tracks historical changes to customer attributes
-- Critical for BSA/AML — KYC status history required
--
-- SCD2 tracked columns:
--   - status (KYC status changes)
--   - credit_score (creditworthiness changes)
--   - credit_tier (tier changes)
--   - city, state (address changes)
--
-- Pattern:
--   1. Hash tracked columns
--   2. Compare incoming hash vs stored hash
--   3. If hash changed → expire old record, insert new
--   4. If hash unchanged → skip (no update needed)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE BANKING_WH;
USE DATABASE BANKING_DB;

-- ── Step 1: Add SCD2 columns to DIM_CUSTOMERS ─────────────────
ALTER TABLE GOLD.DIM_CUSTOMERS ADD COLUMN IF NOT EXISTS
    effective_from    DATE          DEFAULT CURRENT_DATE();

ALTER TABLE GOLD.DIM_CUSTOMERS ADD COLUMN IF NOT EXISTS
    effective_to      DATE          DEFAULT '9999-12-31'::DATE;

ALTER TABLE GOLD.DIM_CUSTOMERS ADD COLUMN IF NOT EXISTS
    is_current        BOOLEAN       DEFAULT TRUE;

ALTER TABLE GOLD.DIM_CUSTOMERS ADD COLUMN IF NOT EXISTS
    record_hash       VARCHAR(32);

-- ── Step 2: Update existing records with hash & SCD2 defaults ─
UPDATE GOLD.DIM_CUSTOMERS
SET
    effective_from = '2024-01-01'::DATE,
    effective_to   = '9999-12-31'::DATE,
    is_current     = TRUE,
    record_hash    = MD5(
        COALESCE(status, '')        ||'|'||
        COALESCE(credit_score::VARCHAR, '') ||'|'||
        COALESCE(credit_tier, '')   ||'|'||
        COALESCE(city, '')          ||'|'||
        COALESCE(state, '')
    )
WHERE is_current IS NULL OR record_hash IS NULL;

-- ── Step 3: Create SCD2 merge procedure ───────────────────────
CREATE OR REPLACE PROCEDURE GOLD.SP_SCD2_DIM_CUSTOMERS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
    rows_inserted   INTEGER DEFAULT 0;
    rows_expired    INTEGER DEFAULT 0;
    rows_unchanged  INTEGER DEFAULT 0;
    result_msg      VARCHAR;
BEGIN

    -- ── Identify changed records ──────────────────────────────
    -- Compare incoming Silver data hash vs stored Gold hash
    CREATE OR REPLACE TEMPORARY TABLE TEMP_SCD2_CHANGES AS
    SELECT
        s.customer_id,
        s.first_name,
        s.last_name,
        s.full_name,
        s.email,
        s.city,
        s.state,
        s.country,
        s.customer_since,
        s.tenure_days,
        CASE
            WHEN s.tenure_days < 365   THEN '0-1 Years'
            WHEN s.tenure_days < 730   THEN '1-2 Years'
            WHEN s.tenure_days < 1825  THEN '2-5 Years'
            WHEN s.tenure_days < 3650  THEN '5-10 Years'
            ELSE '10+ Years'
        END                                     AS tenure_band,
        s.age_years,
        CASE
            WHEN s.age_years < 25 THEN '18-24'
            WHEN s.age_years < 35 THEN '25-34'
            WHEN s.age_years < 45 THEN '35-44'
            WHEN s.age_years < 55 THEN '45-54'
            WHEN s.age_years < 65 THEN '55-64'
            ELSE '65+'
        END                                     AS age_band,
        s.status,
        s.credit_score,
        s.credit_tier,
        CASE WHEN s.status = 'ACTIVE'
            THEN TRUE ELSE FALSE END            AS is_active,
        s._silver_loaded_at,
        -- Compute incoming hash
        MD5(
            COALESCE(s.status, '')              ||'|'||
            COALESCE(s.credit_score::VARCHAR,'') ||'|'||
            COALESCE(s.credit_tier, '')         ||'|'||
            COALESCE(s.city, '')                ||'|'||
            COALESCE(s.state, '')
        )                                       AS new_hash,
        -- Get existing hash from Gold
        d.record_hash                           AS old_hash,
        d.customer_key                          AS existing_key,
        CASE
            WHEN d.customer_id IS NULL THEN 'NEW'
            WHEN d.record_hash != MD5(
                COALESCE(s.status, '')              ||'|'||
                COALESCE(s.credit_score::VARCHAR,'') ||'|'||
                COALESCE(s.credit_tier, '')         ||'|'||
                COALESCE(s.city, '')                ||'|'||
                COALESCE(s.state, '')
            ) THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END                                     AS change_type
    FROM BANKING_DB.SILVER.SILVER_CUSTOMERS s
    LEFT JOIN BANKING_DB.GOLD.DIM_CUSTOMERS d
        ON s.customer_id = d.customer_id
        AND d.is_current = TRUE
    WHERE s.dq_is_valid = TRUE;

    -- ── Expire changed records ────────────────────────────────
    UPDATE BANKING_DB.GOLD.DIM_CUSTOMERS d
    SET
        effective_to = DATEADD('day', -1, CURRENT_DATE()),
        is_current   = FALSE
    FROM TEMP_SCD2_CHANGES t
    WHERE d.customer_id   = t.customer_id
    AND   d.is_current    = TRUE
    AND   t.change_type   = 'CHANGED';

    rows_expired := SQLROWCOUNT;

    -- ── Insert new/changed records ────────────────────────────
    INSERT INTO BANKING_DB.GOLD.DIM_CUSTOMERS (
        customer_key, customer_id,
        first_name, last_name, full_name,
        email, city, state, country,
        customer_since, tenure_days, tenure_band,
        age_years, age_band,
        status, credit_score, credit_tier,
        is_active, _silver_loaded_at, _gold_loaded_at,
        effective_from, effective_to,
        is_current, record_hash
    )
    SELECT
        BANKING_DB.GOLD.SEQ_CUSTOMER_KEY.NEXTVAL,
        customer_id,
        first_name, last_name, full_name,
        email, city, state, country,
        customer_since, tenure_days, tenure_band,
        age_years, age_band,
        status, credit_score, credit_tier,
        is_active, _silver_loaded_at,
        CURRENT_TIMESTAMP(),
        CURRENT_DATE(),
        '9999-12-31'::DATE,
        TRUE,
        new_hash
    FROM TEMP_SCD2_CHANGES
    WHERE change_type IN ('NEW', 'CHANGED');

    rows_inserted := SQLROWCOUNT;

    -- Count unchanged
    SELECT COUNT(*) INTO rows_unchanged
    FROM TEMP_SCD2_CHANGES
    WHERE change_type = 'UNCHANGED';

    -- Build result message
    result_msg :=
        'SCD2 Complete — ' ||
        'Inserted: '  || rows_inserted  || ' | ' ||
        'Expired: '   || rows_expired   || ' | ' ||
        'Unchanged: ' || rows_unchanged;

    -- Log to audit
    INSERT INTO BANKING_DB.AUDIT.PIPELINE_RUN_LOG (
        run_id, pipeline_name, entity, status,
        records_loaded, started_at, completed_at
    )
    VALUES (
        'SCD2_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'),
        'SP_SCD2_DIM_CUSTOMERS',
        'DIM_CUSTOMERS',
        'SUCCESS',
        rows_inserted,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );

    RETURN result_msg;

END;
$$;

-- ── Step 4: Run the SCD2 procedure ───────────────────────────
CALL GOLD.SP_SCD2_DIM_CUSTOMERS();

-- ── Step 5: Verify SCD2 results ──────────────────────────────
-- Current records
SELECT
    'Current Records'   AS record_type,
    COUNT(*)            AS count
FROM GOLD.DIM_CUSTOMERS
WHERE is_current = TRUE
UNION ALL
SELECT
    'Historical Records',
    COUNT(*)
FROM GOLD.DIM_CUSTOMERS
WHERE is_current = FALSE
UNION ALL
SELECT
    'Total Records',
    COUNT(*)
FROM GOLD.DIM_CUSTOMERS;

-- Sample SCD2 records
SELECT
    customer_id,
    full_name,
    status,
    credit_score,
    credit_tier,
    city,
    state,
    effective_from,
    effective_to,
    is_current,
    record_hash
FROM GOLD.DIM_CUSTOMERS
ORDER BY customer_id, effective_from
LIMIT 20;