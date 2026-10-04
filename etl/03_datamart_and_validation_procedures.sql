USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Analytical Data Mart & Validation Layer (Tier 4 & Criterion 3)
-- Script: 03_datamart_and_validation_procedures.sql
-- Description: DDL for departmental data mart and stored procedures for
--              presentation rollup loading and automated validation logging.
-- Criteria Fulfillment: Criterion 3 (Validation Logging & Referential Integrity)
-- ============================================================================

PRINT '================================================================================';
PRINT '  [STEP 3] CREATING DATA MART DDL AND VALIDATION STORED PROCEDURES              ';
PRINT '================================================================================';
GO

-- ----------------------------------------------------------------------------
-- 1. ANALYTICAL DATA MART DDL (mart.marketing_performance)
-- ----------------------------------------------------------------------------
IF OBJECT_ID('mart.marketing_performance', 'U') IS NOT NULL
    DROP TABLE mart.marketing_performance;
GO

CREATE TABLE mart.marketing_performance (
    month_key TINYINT NOT NULL,
    job VARCHAR(50) NOT NULL,
    channel_key TINYINT NOT NULL,
    contacts INT NOT NULL,
    subscriptions INT NOT NULL,
    total_duration_seconds BIGINT NOT NULL,
    PRIMARY KEY (month_key, job, channel_key)
);
GO

PRINT '  [OK] mart.marketing_performance table provisioned.';
GO

-- ----------------------------------------------------------------------------
-- 2. DATA MART POPULATION STORED PROCEDURE
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE etl.usp_20_load_marketing_performance_mart
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE mart.marketing_performance;

    -- Pre-aggregates fact and customer profile metrics via single-integer customer_sk join
    INSERT INTO mart.marketing_performance
        (month_key, job, channel_key, contacts, subscriptions, total_duration_seconds)
    SELECT
        f.month_key,
        p.job,
        f.channel_key,
        SUM(CAST(f.contact_count AS INT)),
        SUM(CAST(f.is_subscribed AS INT)),
        SUM(CAST(f.duration_seconds AS BIGINT))
    FROM dw.fact_contact AS f
    JOIN dw.dim_customer_profile AS p 
        ON f.customer_sk = p.customer_sk
    GROUP BY f.month_key, p.job, f.channel_key;

    DECLARE @MartCnt INT = @@ROWCOUNT;
    PRINT '  [OK] mart.marketing_performance populated (' + CAST(@MartCnt AS VARCHAR) + ' rollup records).';
END;
GO

-- ----------------------------------------------------------------------------
-- 3. AUTOMATED VALIDATION LOGGING & AUDIT ASSERTIONS (Criterion 3)
-- ----------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE etl.usp_30_run_validation_audits
    @ExecutionId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ExecutionId IS NULL
        SET @ExecutionId = NEWID();

    PRINT '================================================================================';
    PRINT '      AUTOMATED VALIDATION LOGGING & INTEGRITY AUDIT (CRITERION 3)              ';
    PRINT '================================================================================';

    -- 1. Gather Metrics
    DECLARE @CrmCnt INT, @CcCnt INT, @CrCnt INT, @FnCnt INT;
    DECLARE @CustSkCount INT, @FactCount INT, @MartContacts INT, @MartSubs INT;
    DECLARE @Pdays999 INT, @PdaysNull INT, @FactSubs INT;
    DECLARE @OrphanCust INT, @OrphanMo INT, @OrphanWd INT, @OrphanCh INT, @OrphanPo INT;

    SELECT @CrmCnt = COUNT(*) FROM stage.retail_banking_crm;
    SELECT @CcCnt = COUNT(*) FROM stage.marketing_call_center;
    SELECT @CrCnt = COUNT(*) FROM stage.credit_and_risk;
    SELECT @FnCnt = COUNT(*) FROM stage.finance_external_feed;

    SELECT @CustSkCount = COUNT(*) FROM dw.dim_customer_profile;
    SELECT @FactCount = COUNT(*) FROM dw.fact_contact;

    SELECT @Pdays999 = COUNT(*) FROM dw.fact_contact WHERE days_since_previous = 999;
    SELECT @PdaysNull = COUNT(*) FROM dw.fact_contact WHERE days_since_previous IS NULL;
    SELECT @FactSubs = SUM(CAST(is_subscribed AS INT)) FROM dw.fact_contact;

    SELECT @OrphanCust = COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_customer_profile p ON f.customer_sk = p.customer_sk WHERE p.customer_sk IS NULL;
    SELECT @OrphanMo   = COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_month m ON f.month_key = m.month_key WHERE m.month_key IS NULL;
    SELECT @OrphanWd   = COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_weekday w ON f.weekday_key = w.weekday_key WHERE w.weekday_key IS NULL;
    SELECT @OrphanCh   = COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_channel c ON f.channel_key = c.channel_key WHERE c.channel_key IS NULL;
    SELECT @OrphanPo   = COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_previous_outcome o ON f.outcome_key = o.outcome_key WHERE o.outcome_key IS NULL;

    SELECT @MartContacts = SUM(contacts), @MartSubs = SUM(subscriptions) FROM mart.marketing_performance;

    -- 2. Record Detailed Audit Results into etl.pipeline_validation_audit
    INSERT INTO etl.pipeline_validation_audit (execution_id, audit_category, metric_name, actual_value, expected_value, audit_status, notes)
    VALUES
        (@ExecutionId, '1. Volume Balance', 'stage.retail_banking_crm', CAST(@CrmCnt AS NVARCHAR), '41188', CASE WHEN @CrmCnt=41188 THEN 'PASS' ELSE 'FAIL' END, 'CRM source landing'),
        (@ExecutionId, '1. Volume Balance', 'stage.marketing_call_center', CAST(@CcCnt AS NVARCHAR), '41188', CASE WHEN @CcCnt=41188 THEN 'PASS' ELSE 'FAIL' END, 'Call center landing'),
        (@ExecutionId, '1. Volume Balance', 'stage.credit_and_risk', CAST(@CrCnt AS NVARCHAR), '41188', CASE WHEN @CrCnt=41188 THEN 'PASS' ELSE 'FAIL' END, 'Credit risk landing'),
        (@ExecutionId, '1. Volume Balance', 'stage.finance_external_feed', CAST(@FnCnt AS NVARCHAR), '41188', CASE WHEN @FnCnt=41188 THEN 'PASS' ELSE 'FAIL' END, 'Finance feed landing'),
        (@ExecutionId, '1. Volume Balance', 'dw.dim_customer_profile', CAST(@CustSkCount AS NVARCHAR), '41188', CASE WHEN @CustSkCount=41188 THEN 'PASS' ELSE 'FAIL' END, 'Customer dimension SKs'),
        (@ExecutionId, '1. Volume Balance', 'dw.fact_contact', CAST(@FactCount AS NVARCHAR), '41188', CASE WHEN @FactCount=41188 THEN 'PASS' ELSE 'FAIL' END, 'Fact event records'),
        (@ExecutionId, '1. Volume Balance', 'mart.marketing_performance (Contacts)', CAST(@MartContacts AS NVARCHAR), '41188', CASE WHEN @MartContacts=41188 THEN 'PASS' ELSE 'FAIL' END, 'Mart aggregated contacts'),

        (@ExecutionId, '2. Cleansing Audits', 'pdays = 999 remaining', CAST(@Pdays999 AS NVARCHAR), '0', CASE WHEN @Pdays999=0 THEN 'PASS' ELSE 'FAIL' END, 'Must be zero 999s remaining'),
        (@ExecutionId, '2. Cleansing Audits', 'pdays cleansed to NULL', CAST(@PdaysNull AS NVARCHAR), '39673', CASE WHEN @PdaysNull=39673 THEN 'PASS' ELSE 'FAIL' END, 'Expected clients not previously contacted'),
        (@ExecutionId, '2. Cleansing Audits', 'Fact subscriptions count', CAST(@FactSubs AS NVARCHAR), '4640', CASE WHEN @FactSubs=4640 THEN 'PASS' ELSE 'FAIL' END, 'Subscription benchmark balance'),
        (@ExecutionId, '2. Cleansing Audits', 'Mart subscriptions count', CAST(@MartSubs AS NVARCHAR), '4640', CASE WHEN @MartSubs=4640 THEN 'PASS' ELSE 'FAIL' END, 'Mart subscription rollup balance'),

        (@ExecutionId, '3. Referential Integrity', 'Orphan Customer FKs', CAST(@OrphanCust AS NVARCHAR), '0', CASE WHEN @OrphanCust=0 THEN 'PASS' ELSE 'FAIL' END, 'Fact to Customer Profile'),
        (@ExecutionId, '3. Referential Integrity', 'Orphan Month FKs', CAST(@OrphanMo AS NVARCHAR), '0', CASE WHEN @OrphanMo=0 THEN 'PASS' ELSE 'FAIL' END, 'Fact to Month Dim'),
        (@ExecutionId, '3. Referential Integrity', 'Orphan Weekday FKs', CAST(@OrphanWd AS NVARCHAR), '0', CASE WHEN @OrphanWd=0 THEN 'PASS' ELSE 'FAIL' END, 'Fact to Weekday Dim'),
        (@ExecutionId, '3. Referential Integrity', 'Orphan Channel FKs', CAST(@OrphanCh AS NVARCHAR), '0', CASE WHEN @OrphanCh=0 THEN 'PASS' ELSE 'FAIL' END, 'Fact to Channel Dim'),
        (@ExecutionId, '3. Referential Integrity', 'Orphan Outcome FKs', CAST(@OrphanPo AS NVARCHAR), '0', CASE WHEN @OrphanPo=0 THEN 'PASS' ELSE 'FAIL' END, 'Fact to Outcome Dim');

    -- 3. Display Formatted Output
    PRINT '  1. Volume & Balance Audit:';
    PRINT '     - Staging CRM Records    : ' + CAST(@CrmCnt AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Staging Call Center    : ' + CAST(@CcCnt AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Staging Credit Risk    : ' + CAST(@CrCnt AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Staging Finance Feed   : ' + CAST(@FnCnt AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Customer Profiles (SK) : ' + CAST(@CustSkCount AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Fact Contact Records   : ' + CAST(@FactCount AS VARCHAR) + ' (Expected: 41,188)';
    PRINT '     - Data Mart Contacts     : ' + CAST(@MartContacts AS VARCHAR) + ' (Expected: 41,188)';

    PRINT '  2. Cleansing & Derived Attributes Audit:';
    PRINT '     - pdays = 999 Remaining : ' + CAST(@Pdays999 AS VARCHAR) + ' (Must be 0)';
    PRINT '     - pdays Cleansed to NULL : ' + CAST(@PdaysNull AS VARCHAR) + ' (Expected: 39,673)';
    PRINT '     - Fact Subscriptions (1) : ' + CAST(@FactSubs AS VARCHAR) + ' (Expected: 4,640)';
    PRINT '     - Mart Subscriptions (1) : ' + CAST(@MartSubs AS VARCHAR) + ' (Expected: 4,640)';

    PRINT '  3. Referential Integrity (Zero Orphan FKs):';
    PRINT '     - Orphan Customer Keys   : ' + CAST(@OrphanCust AS VARCHAR) + ' (Must be 0)';
    PRINT '     - Orphan Month Keys      : ' + CAST(@OrphanMo AS VARCHAR) + ' (Must be 0)';
    PRINT '     - Orphan Weekday Keys    : ' + CAST(@OrphanWd AS VARCHAR) + ' (Must be 0)';
    PRINT '     - Orphan Channel Keys    : ' + CAST(@OrphanCh AS VARCHAR) + ' (Must be 0)';
    PRINT '     - Orphan Outcome Keys    : ' + CAST(@OrphanPo AS VARCHAR) + ' (Must be 0)';

    -- 4. Enforce Hard Assertions (Criterion 3 Gate)
    IF (@CrmCnt != 41188 OR @CcCnt != 41188 OR @CrCnt != 41188 OR @FnCnt != 41188 OR
        @CustSkCount != 41188 OR @FactCount != 41188 OR @MartContacts != 41188 OR
        @Pdays999 != 0 OR @PdaysNull != 39673 OR @FactSubs != 4640 OR @MartSubs != 4640 OR
        @OrphanCust != 0 OR @OrphanMo != 0 OR @OrphanWd != 0 OR @OrphanCh != 0 OR @OrphanPo != 0)
    BEGIN
        THROW 51003, 'CRITERION 3 VALIDATION FAILED: Integrity discrepancies detected across warehouse tiers.', 1;
    END

    PRINT '--------------------------------------------------------------------------------';
    PRINT '  [PASS] CRITERION 3 EVIDENCE VERIFIED: ALL AUDIT ASSERTIONS PASSED WITH ZERO DISCREPANCIES.';
    PRINT '--------------------------------------------------------------------------------';
END;
GO

PRINT '--------------------------------------------------------------------------------';
PRINT '  [STEP 3 COMPLETE] Data mart and validation procedures provisioned.';
PRINT '--------------------------------------------------------------------------------';
GO
