USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse Layer (Tier 3)
-- Script: 02_dimensional_loading_procedures.sql
-- Description: DDL for star-schema dimension and fact tables, plus stored
--              procedures enforcing strict relational order loading and in-flight
--              cleansing (pdays=999 -> NULL, age_band derivation, surrogate keys).
-- Criteria Fulfillment: Criterion 3 (Automated Stage 2 Transform & Stage 3 Load)
-- ============================================================================

PRINT '================================================================================';
PRINT '  [STEP 2] CREATING DIMENSIONAL DDL AND LOADING STORED PROCEDURES               ';
PRINT '================================================================================';
GO

-- ----------------------------------------------------------------------------
-- 1. DIMENSION AND FACT DDLs
-- ----------------------------------------------------------------------------

-- 1.1 dw.dim_month
IF OBJECT_ID('dw.dim_month', 'U') IS NOT NULL
    DROP TABLE dw.dim_month;
GO
CREATE TABLE dw.dim_month (
    month_key TINYINT PRIMARY KEY, 
    month_abbr CHAR(3) NOT NULL UNIQUE, 
    month_name VARCHAR(15) NOT NULL, 
    quarter_no TINYINT NOT NULL
);
GO

-- 1.2 dw.dim_weekday
IF OBJECT_ID('dw.dim_weekday', 'U') IS NOT NULL
    DROP TABLE dw.dim_weekday;
GO
CREATE TABLE dw.dim_weekday (
    weekday_key TINYINT PRIMARY KEY, 
    day_abbr CHAR(3) NOT NULL UNIQUE, 
    weekday_name VARCHAR(10) NOT NULL
);
GO

-- 1.3 dw.dim_channel
IF OBJECT_ID('dw.dim_channel', 'U') IS NOT NULL
    DROP TABLE dw.dim_channel;
GO
CREATE TABLE dw.dim_channel (
    channel_key TINYINT IDENTITY(1,1) PRIMARY KEY, 
    contact_type VARCHAR(20) NOT NULL UNIQUE
);
GO

-- 1.4 dw.dim_previous_outcome
IF OBJECT_ID('dw.dim_previous_outcome', 'U') IS NOT NULL
    DROP TABLE dw.dim_previous_outcome;
GO
CREATE TABLE dw.dim_previous_outcome (
    outcome_key TINYINT IDENTITY(1,1) PRIMARY KEY, 
    previous_outcome VARCHAR(20) NOT NULL UNIQUE
);
GO

-- 1.5 dw.dim_campaign
IF OBJECT_ID('dw.dim_campaign', 'U') IS NOT NULL
    DROP TABLE dw.dim_campaign;
GO
CREATE TABLE dw.dim_campaign (
    campaign_key INT IDENTITY(1,1) PRIMARY KEY,
    campaign_contacts INT NOT NULL UNIQUE,
    campaign_band VARCHAR(20) NOT NULL
);
GO

-- 1.6 dw.dim_customer_profile
IF OBJECT_ID('dw.dim_customer_profile', 'U') IS NOT NULL
    DROP TABLE dw.dim_customer_profile;
GO
CREATE TABLE dw.dim_customer_profile (
    customer_sk INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    age INT NOT NULL,
    age_band VARCHAR(12) NOT NULL,
    job VARCHAR(50),
    marital VARCHAR(20),
    education VARCHAR(30),
    default_status VARCHAR(10),
    housing_loan VARCHAR(10),
    personal_loan VARCHAR(10),
    CONSTRAINT UQ_dim_customer_profile_customer_id UNIQUE (customer_id)
);
GO

-- 1.7 Central Fact Table: dw.fact_contact
IF OBJECT_ID('dw.fact_bank_marketing', 'V') IS NOT NULL
    DROP VIEW dw.fact_bank_marketing;
GO
IF OBJECT_ID('dw.fact_contact', 'U') IS NOT NULL
    DROP TABLE dw.fact_contact;
GO

CREATE TABLE dw.fact_contact (
    contact_id INT IDENTITY(1,1) PRIMARY KEY,
    customer_sk INT NOT NULL,
    month_key TINYINT NOT NULL,
    weekday_key TINYINT NOT NULL,
    channel_key TINYINT NOT NULL,
    outcome_key TINYINT NOT NULL,
    contact_count TINYINT NOT NULL DEFAULT 1,
    is_subscribed TINYINT NOT NULL,
    duration_seconds INT NOT NULL,
    campaign_contacts INT NOT NULL,
    previous_contacts INT NOT NULL,
    days_since_previous INT NULL,
    emp_var_rate DECIMAL(5,2) NULL,
    cons_price_idx DECIMAL(7,3) NULL,
    cons_conf_idx DECIMAL(6,2) NULL,
    euribor3m DECIMAL(7,3) NULL,
    nr_employed DECIMAL(7,1) NULL,

    FOREIGN KEY (customer_sk) REFERENCES dw.dim_customer_profile(customer_sk),
    FOREIGN KEY (month_key) REFERENCES dw.dim_month(month_key),
    FOREIGN KEY (weekday_key) REFERENCES dw.dim_weekday(weekday_key),
    FOREIGN KEY (channel_key) REFERENCES dw.dim_channel(channel_key),
    FOREIGN KEY (outcome_key) REFERENCES dw.dim_previous_outcome(outcome_key)
);
GO

-- Performance indexes for Star Schema Lookups
CREATE NONCLUSTERED INDEX IX_fact_contact_customer ON dw.fact_contact (customer_sk);
CREATE NONCLUSTERED INDEX IX_fact_contact_month ON dw.fact_contact (month_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_weekday ON dw.fact_contact (weekday_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_channel ON dw.fact_contact (channel_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_outcome ON dw.fact_contact (outcome_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_subscription ON dw.fact_contact (is_subscribed) INCLUDE (contact_count, duration_seconds);
GO

-- Standardized view alias
CREATE VIEW dw.fact_bank_marketing AS SELECT * FROM dw.fact_contact;
GO

PRINT '  [OK] Dimensional star schema tables and fact indexes provisioned.';
GO

-- ----------------------------------------------------------------------------
-- 2. DIMENSIONAL LOADING STORED PROCEDURES (Strict Relational Order)
-- ----------------------------------------------------------------------------

-- 2.1 Step 3.1: Date & Time Dimensions
CREATE OR ALTER PROCEDURE etl.usp_10_load_calendar_dimensions
AS
BEGIN
    SET NOCOUNT ON;

    -- Load dw.dim_month
    IF NOT EXISTS (SELECT 1 FROM dw.dim_month)
    BEGIN
        INSERT INTO dw.dim_month (month_key, month_abbr, month_name, quarter_no) VALUES 
            (1,  'jan', 'January',   1),
            (2,  'feb', 'February',  1),
            (3,  'mar', 'March',     1),
            (4,  'apr', 'April',     2),
            (5,  'may', 'May',       2),
            (6,  'jun', 'June',      2),
            (7,  'jul', 'July',      3),
            (8,  'aug', 'August',    3),
            (9,  'sep', 'September', 3),
            (10, 'oct', 'October',   4),
            (11, 'nov', 'November',  4),
            (12, 'dec', 'December',  4);
        PRINT '  [OK] dw.dim_month populated (12 rows).';
    END

    -- Load dw.dim_weekday
    IF NOT EXISTS (SELECT 1 FROM dw.dim_weekday)
    BEGIN
        INSERT INTO dw.dim_weekday (weekday_key, day_abbr, weekday_name) VALUES 
            (1, 'mon', 'Monday'),
            (2, 'tue', 'Tuesday'),
            (3, 'wed', 'Wednesday'),
            (4, 'thu', 'Thursday'),
            (5, 'fri', 'Friday');
        PRINT '  [OK] dw.dim_weekday populated (5 rows).';
    END
END;
GO

-- 2.2 Step 3.2: Entity Dimensions with Surrogate Keys
CREATE OR ALTER PROCEDURE etl.usp_11_load_entity_dimensions
AS
BEGIN
    SET NOCOUNT ON;

    -- 1. dw.dim_channel
    TRUNCATE TABLE dw.dim_channel;
    INSERT INTO dw.dim_channel (contact_type)
    SELECT DISTINCT LTRIM(RTRIM(contact))
    FROM stage.marketing_call_center
    WHERE contact IS NOT NULL
    ORDER BY LTRIM(RTRIM(contact));
    DECLARE @ChCnt INT = @@ROWCOUNT;

    -- 2. dw.dim_previous_outcome
    TRUNCATE TABLE dw.dim_previous_outcome;
    INSERT INTO dw.dim_previous_outcome (previous_outcome)
    SELECT DISTINCT LTRIM(RTRIM(poutcome))
    FROM stage.marketing_call_center
    WHERE poutcome IS NOT NULL
    ORDER BY LTRIM(RTRIM(poutcome));
    DECLARE @PoCnt INT = @@ROWCOUNT;

    -- 3. dw.dim_campaign
    TRUNCATE TABLE dw.dim_campaign;
    INSERT INTO dw.dim_campaign (campaign_contacts, campaign_band)
    SELECT DISTINCT 
        campaign,
        CASE 
            WHEN campaign = 1 THEN '1 contact'
            WHEN campaign BETWEEN 2 AND 3 THEN '2-3 contacts'
            WHEN campaign BETWEEN 4 AND 5 THEN '4-5 contacts'
            ELSE '6+ contacts'
        END AS campaign_band
    FROM stage.marketing_call_center
    ORDER BY campaign;
    DECLARE @CmpCnt INT = @@ROWCOUNT;

    PRINT '  [OK] Entity dimensions populated with surrogate keys (Channels: ' 
          + CAST(@ChCnt AS VARCHAR) + ', Outcomes: ' + CAST(@PoCnt AS VARCHAR) 
          + ', Campaigns: ' + CAST(@CmpCnt AS VARCHAR) + ').';
END;
GO

-- 2.3 Step 3.2 (cont): Customer Profile Dimension
CREATE OR ALTER PROCEDURE etl.usp_12_load_customer_profile
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dw.dim_customer_profile;

    -- In-engine transformation: Deriving age_band, normalizing strings, surrogate keys
    INSERT INTO dw.dim_customer_profile (
        customer_id, age, age_band, job, marital, education,
        default_status, housing_loan, personal_loan
    )
    SELECT
        c.customer_id,
        c.age,
        CASE
            WHEN c.age < 30 THEN 'Under 30'
            WHEN c.age < 40 THEN '30-39'
            WHEN c.age < 50 THEN '40-49'
            WHEN c.age < 60 THEN '50-59'
            ELSE '60 and over'
        END AS age_band,
        LTRIM(RTRIM(c.job)), 
        LTRIM(RTRIM(c.marital)), 
        LTRIM(RTRIM(c.education)),
        LTRIM(RTRIM(r.[default])), 
        LTRIM(RTRIM(r.housing)), 
        LTRIM(RTRIM(r.loan))
    FROM stage.retail_banking_crm AS c
    JOIN stage.credit_and_risk AS r 
        ON c.customer_id = r.customer_id
    ORDER BY c.customer_id;

    DECLARE @CustCnt INT = @@ROWCOUNT;
    PRINT '  [OK] dw.dim_customer_profile populated with surrogate keys & age bands (' + CAST(@CustCnt AS VARCHAR) + ' rows).';
END;
GO

-- 2.4 Step 3.3: Central Fact Table Population with Surrogate Key Lookups
CREATE OR ALTER PROCEDURE etl.usp_13_load_fact_contact
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dw.fact_contact;

    -- Resolves surrogate keys via dimension lookups and cleanses pdays=999 -> NULL
    INSERT INTO dw.fact_contact (
        customer_sk, month_key, weekday_key, channel_key, outcome_key,
        contact_count, is_subscribed, duration_seconds,
        campaign_contacts, previous_contacts, days_since_previous,
        emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
    )
    SELECT
        p.customer_sk,
        mo.month_key,
        wd.weekday_key,
        ch.channel_key,
        po.outcome_key,
        1 AS contact_count,
        CASE WHEN m.y = 'yes' THEN 1 ELSE 0 END AS is_subscribed,
        m.duration AS duration_seconds,
        m.campaign AS campaign_contacts,
        m.[previous] AS previous_contacts,
        CASE WHEN m.pdays = 999 THEN NULL ELSE m.pdays END AS days_since_previous,
        e.emp_var_rate,
        e.cons_price_idx,
        e.cons_conf_idx,
        e.euribor3m,
        e.nr_employed
    FROM stage.marketing_call_center AS m
    JOIN dw.dim_customer_profile AS p
        ON m.customer_id = p.customer_id
    JOIN stage.finance_external_feed AS e
        ON m.customer_id = e.customer_id
    JOIN dw.dim_month AS mo
        ON mo.month_abbr = m.[month]
    JOIN dw.dim_weekday AS wd
        ON wd.day_abbr = m.day_of_week
    JOIN dw.dim_channel AS ch
        ON ch.contact_type = m.contact
    JOIN dw.dim_previous_outcome AS po
        ON po.previous_outcome = m.poutcome;

    DECLARE @FactCnt INT = @@ROWCOUNT;
    PRINT '  [OK] dw.fact_contact / dw.fact_bank_marketing populated successfully (' + CAST(@FactCnt AS VARCHAR) + ' records).';
END;
GO

PRINT '--------------------------------------------------------------------------------';
PRINT '  [STEP 2 COMPLETE] Dimensional loading procedures provisioned.';
PRINT '--------------------------------------------------------------------------------';
GO
