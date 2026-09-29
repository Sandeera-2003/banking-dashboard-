USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Staging Storage Layer (Tier 2)
-- Namespace: stage
-- Characteristics: Raw, unindexed, temporary tables matching source file structures.
-- Purpose: Isolated landing zone for all operational and external feeds prior
--          to ETL transformation and warehouse loading.
-- ============================================================================

-- Ensure the staging schema exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage')
BEGIN
    EXEC('CREATE SCHEMA stage;');
END
GO

-- ----------------------------------------------------------------------------
-- 1. Table: stage.retail_banking_crm
-- Source: Retail Banking Core CRM System (dataSources/retail_banking_crm.sql)
-- Characteristics: Unindexed Heap Table (No PK/Clustered Index, No FKs)
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 2. Table: stage.marketing_call_center
-- Source: Telemarketing Contact Center (dataSources/marketing_call_center.csv)
-- Characteristics: Unindexed Heap Table
-- ----------------------------------------------------------------------------
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

-- Standardized synonym/view alias for architectural naming flexibility
CREATE VIEW stage.call_logs AS
SELECT * FROM stage.marketing_call_center;
GO

-- ----------------------------------------------------------------------------
-- 3. Table: stage.credit_and_risk
-- Source: Credit Risk Department (dataSources/credit_and_risk.csv / .xlsx)
-- Characteristics: Unindexed Heap Table
-- ----------------------------------------------------------------------------
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

-- ----------------------------------------------------------------------------
-- 4. Table: stage.finance_external_feed
-- Source: Macroeconomic External Feed (dataSources/finance_external_feed.json / .csv)
-- Characteristics: Unindexed Heap Table
-- ----------------------------------------------------------------------------
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

-- Standardized synonym/view alias for architectural naming flexibility
CREATE VIEW stage.external_economics AS
SELECT * FROM stage.finance_external_feed;
GO

PRINT 'Staging storage schema and unindexed landing tables created successfully.';
GO
