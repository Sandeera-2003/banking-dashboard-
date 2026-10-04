USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Staging Ingestion Layer (Tier 1 -> Tier 2)
-- Script: 01_stage_ingestion_procedures.sql
-- Description: DDL for unindexed staging heap tables and stored procedures for
--              automated ingestion (BULK INSERT, native OPENJSON, Parity Audits).
-- Criteria Fulfillment: Criterion 3 (Automated Stage 1 Extract & Parity Validation)
-- ============================================================================

PRINT '================================================================================';
PRINT '  [STEP 1] CREATING STAGING DDL AND INGESTION STORED PROCEDURES                 ';
PRINT '================================================================================';
GO

-- ----------------------------------------------------------------------------
-- 1. STAGING TABLE DDLs (Unindexed Transient Heaps)
-- ----------------------------------------------------------------------------

-- 1.1 stage.retail_banking_crm
IF OBJECT_ID('stage.retail_banking_crm', 'U') IS NOT NULL
    DROP TABLE stage.retail_banking_crm;
GO
CREATE TABLE stage.retail_banking_crm (
    customer_id INT NOT NULL,
    age INT NOT NULL,
    job VARCHAR(50),
    marital VARCHAR(20),
    education VARCHAR(30)
);
GO

-- 1.2 stage.marketing_call_center
IF OBJECT_ID('stage.call_logs', 'V') IS NOT NULL
    DROP VIEW stage.call_logs;
GO
IF OBJECT_ID('stage.marketing_call_center', 'U') IS NOT NULL
    DROP TABLE stage.marketing_call_center;
GO
CREATE TABLE stage.marketing_call_center (
    customer_id INT NOT NULL,
    contact VARCHAR(20),
    [month] VARCHAR(10),
    day_of_week VARCHAR(10),
    duration INT,
    campaign INT,
    pdays INT,
    [previous] INT,
    poutcome VARCHAR(20),
    y VARCHAR(10)
);
GO
CREATE VIEW stage.call_logs AS SELECT * FROM stage.marketing_call_center;
GO

-- 1.3 stage.credit_and_risk
IF OBJECT_ID('stage.credit_and_risk', 'U') IS NOT NULL
    DROP TABLE stage.credit_and_risk;
GO
CREATE TABLE stage.credit_and_risk (
    customer_id INT NOT NULL,
    [default] VARCHAR(10),
    housing VARCHAR(10),
    loan VARCHAR(10)
);
GO

-- 1.4 stage.finance_external_feed
IF OBJECT_ID('stage.external_economics', 'V') IS NOT NULL
    DROP VIEW stage.external_economics;
GO
IF OBJECT_ID('stage.finance_external_feed', 'U') IS NOT NULL
    DROP TABLE stage.finance_external_feed;
GO
CREATE TABLE stage.finance_external_feed (
    customer_id INT NOT NULL,
    emp_var_rate DECIMAL(5,2),
    cons_price_idx DECIMAL(7,3),
    cons_conf_idx DECIMAL(6,2),
    euribor3m DECIMAL(7,3),
    nr_employed DECIMAL(7,1)
);
GO
CREATE VIEW stage.external_economics AS SELECT * FROM stage.finance_external_feed;
GO

PRINT '  [OK] Staging tables and views created successfully.';
GO

-- ----------------------------------------------------------------------------
-- 2. STAGING INGESTION STORED PROCEDURES
-- ----------------------------------------------------------------------------

-- 2.1 Ingest Marketing Call Center CSV
CREATE OR ALTER PROCEDURE etl.usp_01_ingest_call_center
    @CsvPath NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @CsvPath IS NULL
    BEGIN
        SELECT @CsvPath = config_value + '/marketing_call_center.csv' 
        FROM etl.pipeline_config WHERE config_key = 'DATASOURCES_PATH';
    END

    TRUNCATE TABLE stage.marketing_call_center;

    DECLARE @Sql NVARCHAR(MAX);
    SET @Sql = N'
    BULK INSERT stage.marketing_call_center
    FROM ''' + REPLACE(@CsvPath, '''', '''''') + N'''
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''0x0a'',
        CODEPAGE = ''65001'',
        TABLOCK
    );';

    EXEC sp_executesql @Sql;
    
    DECLARE @Cnt INT = @@ROWCOUNT;
    PRINT '  [OK] stage.marketing_call_center ingested (' + CAST(@Cnt AS VARCHAR) + ' rows).';
END;
GO

-- 2.2 Ingest Credit and Risk CSV
CREATE OR ALTER PROCEDURE etl.usp_02_ingest_credit_risk
    @CsvPath NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @CsvPath IS NULL
    BEGIN
        SELECT @CsvPath = config_value + '/credit_and_risk.csv' 
        FROM etl.pipeline_config WHERE config_key = 'DATASOURCES_PATH';
    END

    TRUNCATE TABLE stage.credit_and_risk;

    DECLARE @Sql NVARCHAR(MAX);
    SET @Sql = N'
    BULK INSERT stage.credit_and_risk
    FROM ''' + REPLACE(@CsvPath, '''', '''''') + N'''
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''0x0a'',
        CODEPAGE = ''65001'',
        TABLOCK
    );';

    EXEC sp_executesql @Sql;
    
    DECLARE @Cnt INT = @@ROWCOUNT;
    PRINT '  [OK] stage.credit_and_risk ingested (' + CAST(@Cnt AS VARCHAR) + ' rows).';
END;
GO

-- 2.3 Ingest Macroeconomic External Feed (Native OPENJSON with BULK INSERT fallback)
CREATE OR ALTER PROCEDURE etl.usp_03_ingest_external_economics
    @DataSourcesPath NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @DataSourcesPath IS NULL
    BEGIN
        SELECT @DataSourcesPath = config_value 
        FROM etl.pipeline_config WHERE config_key = 'DATASOURCES_PATH';
    END

    TRUNCATE TABLE stage.finance_external_feed;

    DECLARE @JsonPath NVARCHAR(500) = @DataSourcesPath + '/finance_external_feed.json';
    DECLARE @CsvPath NVARCHAR(500) = @DataSourcesPath + '/finance_external_feed.csv';
    DECLARE @Sql NVARCHAR(MAX);
    DECLARE @Loaded INT = 0;

    -- Try 1: Native OPENJSON (Eliminates python json_to_csv converter)
    BEGIN TRY
        SET @Sql = N'
        INSERT INTO stage.finance_external_feed (
            customer_id, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
        )
        SELECT 
            customer_id, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
        FROM OPENROWSET(BULK ''' + REPLACE(@JsonPath, '''', '''''') + N''', SINGLE_CLOB) AS j
        CROSS APPLY OPENJSON(BulkColumn)
        WITH (
            customer_id INT ''$.customer_id'',
            emp_var_rate DECIMAL(5,2) ''$."emp.var.rate"'',
            cons_price_idx DECIMAL(7,3) ''$."cons.price.idx"'',
            cons_conf_idx DECIMAL(6,2) ''$."cons.conf.idx"'',
            euribor3m DECIMAL(7,3) ''$."euribor3m"'',
            nr_employed DECIMAL(7,1) ''$."nr.employed"''
        );';
        
        EXEC sp_executesql @Sql;
        SET @Loaded = @@ROWCOUNT;
        PRINT '  [OK] stage.finance_external_feed ingested via native OPENJSON (' + CAST(@Loaded AS VARCHAR) + ' rows).';
    END TRY
    BEGIN CATCH
        -- Fallback: If OPENROWSET(BULK) encounters filesystem security restrictions, bulk insert normalized CSV
        PRINT '  [WARN] OPENJSON direct read bypassed. Falling back to normalized CSV BULK INSERT...';
        SET @Sql = N'
        BULK INSERT stage.finance_external_feed
        FROM ''' + REPLACE(@CsvPath, '''', '''''') + N'''
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = '','',
            ROWTERMINATOR = ''0x0a'',
            CODEPAGE = ''65001'',
            TABLOCK
        );';
        EXEC sp_executesql @Sql;
        SET @Loaded = @@ROWCOUNT;
        PRINT '  [OK] stage.finance_external_feed ingested via CSV fallback (' + CAST(@Loaded AS VARCHAR) + ' rows).';
    END CATCH;
END;
GO

-- 2.4 Staging Parity Verification & Assertion
CREATE OR ALTER PROCEDURE etl.usp_04_verify_staging_parity
    @ExecutionId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @CrmCnt INT, @CcCnt INT, @CrCnt INT, @FnCnt INT;
    DECLARE @Expected INT = 41188;

    SELECT @CrmCnt = COUNT(*) FROM stage.retail_banking_crm;
    SELECT @CcCnt = COUNT(*) FROM stage.marketing_call_center;
    SELECT @CrCnt = COUNT(*) FROM stage.credit_and_risk;
    SELECT @FnCnt = COUNT(*) FROM stage.finance_external_feed;

    -- Log to audit table if ExecutionId is provided
    IF @ExecutionId IS NOT NULL
    BEGIN
        INSERT INTO etl.pipeline_validation_audit (execution_id, audit_category, metric_name, actual_value, expected_value, audit_status, notes)
        VALUES 
            (@ExecutionId, 'Stage 1: Parity', 'stage.retail_banking_crm', CAST(@CrmCnt AS NVARCHAR), CAST(@Expected AS NVARCHAR), CASE WHEN @CrmCnt = @Expected THEN 'PASS' ELSE 'FAIL' END, 'CRM source rows'),
            (@ExecutionId, 'Stage 1: Parity', 'stage.marketing_call_center', CAST(@CcCnt AS NVARCHAR), CAST(@Expected AS NVARCHAR), CASE WHEN @CcCnt = @Expected THEN 'PASS' ELSE 'FAIL' END, 'Call center rows'),
            (@ExecutionId, 'Stage 1: Parity', 'stage.credit_and_risk', CAST(@CrCnt AS NVARCHAR), CAST(@Expected AS NVARCHAR), CASE WHEN @CrCnt = @Expected THEN 'PASS' ELSE 'FAIL' END, 'Credit risk rows'),
            (@ExecutionId, 'Stage 1: Parity', 'stage.finance_external_feed', CAST(@FnCnt AS NVARCHAR), CAST(@Expected AS NVARCHAR), CASE WHEN @FnCnt = @Expected THEN 'PASS' ELSE 'FAIL' END, 'Macroeconomic feed rows');
    END

    IF (@CrmCnt != @Expected OR @CcCnt != @Expected OR @CrCnt != @Expected OR @FnCnt != @Expected)
    BEGIN
        DECLARE @ErrMsg NVARCHAR(500) = FORMATMESSAGE(
            'Stage 1 Parity Check Failed! Expected %d rows across all feeds. Actual: CRM=%d, CallCenter=%d, Credit=%d, Finance=%d.',
            @Expected, @CrmCnt, @CcCnt, @CrCnt, @FnCnt
        );
        THROW 51001, @ErrMsg, 1;
    END

    PRINT '  [PASS] Staging Row Parity Verified: Exactly 41,188 rows across all 4 staging feeds.';
END;
GO

PRINT '--------------------------------------------------------------------------------';
PRINT '  [STEP 1 COMPLETE] Staging ingestion procedures provisioned.';
PRINT '--------------------------------------------------------------------------------';
GO
