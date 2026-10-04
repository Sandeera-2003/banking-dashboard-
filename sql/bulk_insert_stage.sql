USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Data Integration Layer -> Staging Storage Layer (Tier 2)
-- Script: bulk_insert_stage.sql
-- Description: Ingests raw source CSVs and JSON into unindexed stage tables
--              using high-performance BULK INSERT and native OPENJSON.
-- ============================================================================

-- 1. Ingest marketing call center logs (CSV with UTF-8 Codepage)
TRUNCATE TABLE stage.marketing_call_center;
BULK INSERT stage.marketing_call_center
FROM '/Users/moni/Desktop/DWBI proj/dataSources/marketing_call_center.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    TABLOCK
);
GO
PRINT '  [OK] stage.marketing_call_center bulk ingested.';
GO

-- 2. Ingest credit and risk data (CSV with UTF-8 Codepage)
TRUNCATE TABLE stage.credit_and_risk;
BULK INSERT stage.credit_and_risk
FROM '/Users/moni/Desktop/DWBI proj/dataSources/credit_and_risk.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',
    TABLOCK
);
GO
PRINT '  [OK] stage.credit_and_risk bulk ingested.';
GO

-- 3. Ingest finance external feed data (Native OPENJSON from JSON source)
TRUNCATE TABLE stage.finance_external_feed;
BEGIN TRY
    INSERT INTO stage.finance_external_feed (
        customer_id, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
    )
    SELECT 
        customer_id, emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
    FROM OPENROWSET(BULK '/Users/moni/Desktop/DWBI proj/dataSources/finance_external_feed.json', SINGLE_CLOB) AS j
    CROSS APPLY OPENJSON(BulkColumn)
    WITH (
        customer_id INT '$.customer_id',
        emp_var_rate DECIMAL(5,2) '$."emp.var.rate"',
        cons_price_idx DECIMAL(7,3) '$."cons.price.idx"',
        cons_conf_idx DECIMAL(6,2) '$."cons.conf.idx"',
        euribor3m DECIMAL(7,3) '$."euribor3m"',
        nr_employed DECIMAL(7,1) '$."nr.employed"'
    );
    PRINT '  [OK] stage.finance_external_feed ingested via native OPENJSON.';
END TRY
BEGIN CATCH
    -- Fallback to CSV bulk insert if OPENROWSET filesystem permissions are restricted
    PRINT '  [WARN] OPENROWSET bypassed, loading via normalized CSV...';
    BULK INSERT stage.finance_external_feed
    FROM '/Users/moni/Desktop/DWBI proj/dataSources/finance_external_feed.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        CODEPAGE = '65001',
        TABLOCK
    );
    PRINT '  [OK] stage.finance_external_feed ingested via CSV fallback.';
END CATCH;
GO

PRINT 'All external feeds loaded into stage schema successfully.';
GO
