USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Data Integration Layer -> Staging Storage Layer (Tier 2)
-- Script: bulk_insert_stage.sql
-- Description: Ingests raw source CSVs into unindexed stage tables
-- ============================================================================

-- 1. Ingest marketing call center logs
TRUNCATE TABLE stage.marketing_call_center;
BULK INSERT stage.marketing_call_center
FROM '/Users/moni/Desktop/DWBI proj/dataSources/marketing_call_center.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

-- 2. Ingest credit and risk data
TRUNCATE TABLE stage.credit_and_risk;
BULK INSERT stage.credit_and_risk
FROM '/Users/moni/Desktop/DWBI proj/dataSources/credit_and_risk.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

-- 3. Ingest finance external feed data
TRUNCATE TABLE stage.finance_external_feed;
BULK INSERT stage.finance_external_feed
FROM '/Users/moni/Desktop/DWBI proj/dataSources/finance_external_feed.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO

PRINT 'All external feeds loaded into stage schema successfully.';
GO
