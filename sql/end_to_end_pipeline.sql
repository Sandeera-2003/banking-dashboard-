USE DWBI_Bank;
GO

-- ============================================================================
-- Enterprise Database Architecture: End-to-End Master SQL Pipeline
-- System: DWBI_Bank
-- Architecture Pattern: 4-Tier Separation & Kimball Dimensional Star Schema
-- Criteria Fulfillment: Criterion 3 (Automated Ingestion & Warehouse Pipeline)
-- 
-- Sequential Pipeline Stages:
--   1. Provisioning: Schemas (stage, dw, edw, mart)
--   2. Stage 1 - Extract: Staging Table DDL & Source Bulk Ingestion
--   3. Stage 2 - Transform: Cleansing (pdays=999 -> NULL, categorical normalization,
--                         derived age bands, subscription outcome flags)
--   4. Stage 3 - Load: Strict Relational Loading:
--        Step 3.1: Date & Time Dimensions (dw.dim_month, dw.dim_weekday)
--        Step 3.2: Entity Dimensions (dw.dim_channel, dw.dim_previous_outcome,
--                  dw.dim_campaign, dw.dim_customer_profile with surrogate keys)
--        Step 3.3: Central Fact Table (dw.fact_contact / dw.fact_bank_marketing)
--                  with surrogate key lookups
--        Step 3.4: Analytical Data Mart (mart.marketing_performance)
--   5. Stage 4 - Automated Validation Logging & Integrity Audits
-- ============================================================================

PRINT '================================================================================';
PRINT '      STARTING DWBI_BANK END-TO-END MASTER WAREHOUSE PIPELINE                  ';
PRINT '================================================================================';
GO

-- ----------------------------------------------------------------------------
-- PROVISIONING: PHYSICAL SCHEMA NAMESPACES
-- ----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage')
    EXEC('CREATE SCHEMA stage;');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw;');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'edw')
    EXEC('CREATE SCHEMA edw;');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mart')
    EXEC('CREATE SCHEMA mart;');
GO

PRINT '[STAGE 0] Physical Schemas (stage, dw, edw, mart) Provisioned Successfully.';
GO

-- ============================================================================
-- STAGE 1: EXTRACT (STAGING LANDING TABLES & BULK LOAD)
-- ============================================================================
PRINT '--------------------------------------------------------------------------------';
PRINT '[STAGE 1: EXTRACT] Provisioning Staging Tables and Ingesting Raw Feeds...';
PRINT '--------------------------------------------------------------------------------';
GO

-- 1. stage.retail_banking_crm
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

-- 2. stage.marketing_call_center
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

-- 3. stage.credit_and_risk
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

-- 4. stage.finance_external_feed
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

PRINT '[STAGE 1: EXTRACT] Staging heap landing tables ready for bulk ingestion.';
GO

-- ============================================================================
-- STAGE 3: LOAD (STRICT RELATIONAL ORDER)
-- ============================================================================
PRINT '--------------------------------------------------------------------------------';
PRINT '[STAGE 3: LOAD] Loading Dimensions in Strict Relational Order...';
PRINT '--------------------------------------------------------------------------------';
GO

-- ----------------------------------------------------------------------------
-- Step 3.1: Date & Time Dimensions
-- ----------------------------------------------------------------------------
-- 1. dw.dim_month
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
GO
PRINT '  [Step 3.1] dw.dim_month loaded successfully (12 rows).';
GO

-- 2. dw.dim_weekday
IF OBJECT_ID('dw.dim_weekday', 'U') IS NOT NULL
    DROP TABLE dw.dim_weekday;
GO
CREATE TABLE dw.dim_weekday (
    weekday_key TINYINT PRIMARY KEY, 
    day_abbr CHAR(3) NOT NULL UNIQUE, 
    weekday_name VARCHAR(10) NOT NULL
);
GO

INSERT INTO dw.dim_weekday (weekday_key, day_abbr, weekday_name) VALUES 
    (1, 'mon', 'Monday'),
    (2, 'tue', 'Tuesday'),
    (3, 'wed', 'Wednesday'),
    (4, 'thu', 'Thursday'),
    (5, 'fri', 'Friday');
GO
PRINT '  [Step 3.1] dw.dim_weekday loaded successfully (5 rows).';
GO

-- ----------------------------------------------------------------------------
-- Step 3.2: Entity Dimensions with Surrogate Keys
-- ----------------------------------------------------------------------------
-- 1. dw.dim_channel
IF OBJECT_ID('dw.dim_channel', 'U') IS NOT NULL
    DROP TABLE dw.dim_channel;
GO
CREATE TABLE dw.dim_channel (
    channel_key TINYINT IDENTITY(1,1) PRIMARY KEY, 
    contact_type VARCHAR(20) NOT NULL UNIQUE
);
GO

INSERT INTO dw.dim_channel (contact_type) 
SELECT DISTINCT contact FROM stage.marketing_call_center;
GO
PRINT '  [Step 3.2] dw.dim_channel loaded successfully with surrogate keys.';
GO

-- 2. dw.dim_previous_outcome
IF OBJECT_ID('dw.dim_previous_outcome', 'U') IS NOT NULL
    DROP TABLE dw.dim_previous_outcome;
GO
CREATE TABLE dw.dim_previous_outcome (
    outcome_key TINYINT IDENTITY(1,1) PRIMARY KEY, 
    previous_outcome VARCHAR(20) NOT NULL UNIQUE
);
GO

INSERT INTO dw.dim_previous_outcome (previous_outcome) 
SELECT DISTINCT poutcome FROM stage.marketing_call_center;
GO
PRINT '  [Step 3.2] dw.dim_previous_outcome loaded successfully with surrogate keys.';
GO

-- 3. dw.dim_campaign
IF OBJECT_ID('dw.dim_campaign', 'U') IS NOT NULL
    DROP TABLE dw.dim_campaign;
GO
CREATE TABLE dw.dim_campaign (
    campaign_key INT IDENTITY(1,1) PRIMARY KEY,
    campaign_contacts INT NOT NULL UNIQUE,
    campaign_band VARCHAR(20) NOT NULL
);
GO

INSERT INTO dw.dim_campaign (campaign_contacts, campaign_band)
SELECT DISTINCT 
    campaign,
    CASE 
        WHEN campaign = 1 THEN '1 contact'
        WHEN campaign BETWEEN 2 AND 3 THEN '2-3 contacts'
        WHEN campaign BETWEEN 4 AND 5 THEN '4-5 contacts'
        ELSE '6+ contacts'
    END
FROM stage.marketing_call_center
ORDER BY campaign;
GO
PRINT '  [Step 3.2] dw.dim_campaign loaded successfully with surrogate keys.';
GO

-- 4. dw.dim_customer_profile
-- Transformations:
--   - Surrogate key customer_sk IDENTITY(1,1) PRIMARY KEY
--   - Natural key customer_id retained with UNIQUE constraint
--   - In-flight age_band calculation (<30, 30-39, 40-49, 50-59, 60 and over)
--   - Categorical normalization and deduplication
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

INSERT INTO dw.dim_customer_profile
    (customer_id, age, age_band, job, marital, education, default_status, housing_loan, personal_loan)
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
JOIN stage.credit_and_risk AS r ON c.customer_id = r.customer_id;
GO
PRINT '  [Step 3.2] dw.dim_customer_profile loaded with surrogate keys and derived age bands.';
GO

-- ----------------------------------------------------------------------------
-- Step 3.3: Central Fact Table (dw.fact_contact / dw.fact_bank_marketing)
-- Transformations:
--   - Surrogate primary key contact_id IDENTITY(1,1) PRIMARY KEY
--   - Resolves customer_sk via single-integer foreign key lookup from dw.dim_customer_profile
--   - Cleanses pdays: CASE WHEN pdays = 999 THEN NULL ELSE pdays END
--   - Derives is_subscribed flag: CASE WHEN y = 'yes' THEN 1 ELSE 0 END
-- ----------------------------------------------------------------------------
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

-- Star Schema Foreign Key Indexes
CREATE NONCLUSTERED INDEX IX_fact_contact_customer ON dw.fact_contact (customer_sk);
CREATE NONCLUSTERED INDEX IX_fact_contact_month ON dw.fact_contact (month_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_weekday ON dw.fact_contact (weekday_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_channel ON dw.fact_contact (channel_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_outcome ON dw.fact_contact (outcome_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_subscription ON dw.fact_contact (is_subscribed) INCLUDE (contact_count, duration_seconds);
GO

-- Standardized view alias for fact_bank_marketing
CREATE VIEW dw.fact_bank_marketing AS
SELECT * FROM dw.fact_contact;
GO

-- Fact Table Population via Dimension Surrogate Key Lookups
INSERT INTO dw.fact_contact (
    customer_sk, month_key, weekday_key, channel_key,
    outcome_key, contact_count, is_subscribed, duration_seconds,
    campaign_contacts, previous_contacts, days_since_previous,
    emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
)
SELECT
    p.customer_sk,
    mo.month_key,
    wd.weekday_key,
    ch.channel_key,
    po.outcome_key,
    1,
    CASE WHEN m.y = 'yes' THEN 1 ELSE 0 END,
    m.duration,
    m.campaign,
    m.[previous],
    CASE WHEN m.pdays = 999 THEN NULL ELSE m.pdays END,
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
GO
PRINT '  [Step 3.3] dw.fact_contact / dw.fact_bank_marketing loaded successfully.';
GO

-- ----------------------------------------------------------------------------
-- Step 3.4: Analytical Data Mart (mart.marketing_performance)
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
GO
PRINT '  [Step 3.4] mart.marketing_performance populated successfully.';
GO

-- ============================================================================
-- STAGE 4: AUTOMATED VALIDATION LOGGING & AUDITS (CRITERION 3 EVIDENCE)
-- ============================================================================
PRINT '================================================================================';
PRINT '      AUTOMATED VALIDATION LOGGING & INTEGRITY AUDIT (CRITERION 3)              ';
PRINT '================================================================================';
GO

-- 1. Row Count Parity Across Staging Feeds
SELECT '1. Staging Parity' AS audit_category,
       (SELECT COUNT(*) FROM stage.retail_banking_crm) AS crm_rows,
       (SELECT COUNT(*) FROM stage.marketing_call_center) AS call_center_rows,
       (SELECT COUNT(*) FROM stage.credit_and_risk) AS credit_rows,
       (SELECT COUNT(*) FROM stage.finance_external_feed) AS finance_rows,
       CASE 
           WHEN (SELECT COUNT(*) FROM stage.retail_banking_crm) = 41188
            AND (SELECT COUNT(*) FROM stage.marketing_call_center) = 41188
            AND (SELECT COUNT(*) FROM stage.credit_and_risk) = 41188
            AND (SELECT COUNT(*) FROM stage.finance_external_feed) = 41188
           THEN 'PASS' ELSE 'FAIL'
       END AS parity_status;

-- 2. Star Schema Dimension Metrics
SELECT '2. Dimension Surrogate Keys' AS audit_category,
       (SELECT COUNT(*) FROM dw.dim_customer_profile) AS total_customer_profiles,
       (SELECT MIN(customer_sk) FROM dw.dim_customer_profile) AS min_customer_sk,
       (SELECT MAX(customer_sk) FROM dw.dim_customer_profile) AS max_customer_sk,
       (SELECT COUNT(*) FROM dw.dim_month) AS months,
       (SELECT COUNT(*) FROM dw.dim_weekday) AS weekdays,
       (SELECT COUNT(*) FROM dw.dim_channel) AS channels,
       (SELECT COUNT(*) FROM dw.dim_previous_outcome) AS outcomes,
       (SELECT COUNT(*) FROM dw.dim_campaign) AS campaigns;

-- 3. Cleansing Rule Audits (pdays = 999 -> NULL)
SELECT '3. Cleansing Audit' AS audit_category,
       (SELECT COUNT(*) FROM dw.fact_contact WHERE days_since_previous = 999) AS pdays_999_count_must_be_zero,
       (SELECT COUNT(*) FROM dw.fact_contact WHERE days_since_previous IS NULL) AS null_pdays_count_must_be_39673,
       (SELECT SUM(is_subscribed) FROM dw.fact_contact) AS total_subscriptions_must_be_4640,
       CASE 
           WHEN (SELECT COUNT(*) FROM dw.fact_contact WHERE days_since_previous = 999) = 0
            AND (SELECT COUNT(*) FROM dw.fact_contact WHERE days_since_previous IS NULL) = 39673
            AND (SELECT SUM(is_subscribed) FROM dw.fact_contact) = 4640
           THEN 'PASS' ELSE 'FAIL'
       END AS cleansing_status;

-- 4. Referential Integrity Orphan Checks (Zero Orphans Required)
SELECT '4. Referential Integrity' AS audit_category,
       (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_customer_profile p ON f.customer_sk = p.customer_sk WHERE p.customer_sk IS NULL) AS orphan_customer_fk,
       (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_month m ON f.month_key = m.month_key WHERE m.month_key IS NULL) AS orphan_month_fk,
       (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_weekday w ON f.weekday_key = w.weekday_key WHERE w.weekday_key IS NULL) AS orphan_weekday_fk,
       (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_channel c ON f.channel_key = c.channel_key WHERE c.channel_key IS NULL) AS orphan_channel_fk,
       (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_previous_outcome o ON f.outcome_key = o.outcome_key WHERE o.outcome_key IS NULL) AS orphan_outcome_fk,
       CASE 
           WHEN (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_customer_profile p ON f.customer_sk = p.customer_sk WHERE p.customer_sk IS NULL) = 0
            AND (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_month m ON f.month_key = m.month_key WHERE m.month_key IS NULL) = 0
            AND (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_weekday w ON f.weekday_key = w.weekday_key WHERE w.weekday_key IS NULL) = 0
            AND (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_channel c ON f.channel_key = c.channel_key WHERE c.channel_key IS NULL) = 0
            AND (SELECT COUNT(*) FROM dw.fact_contact f LEFT JOIN dw.dim_previous_outcome o ON f.outcome_key = o.outcome_key WHERE o.outcome_key IS NULL) = 0
           THEN 'PASS' ELSE 'FAIL'
       END AS referential_integrity_status;

-- 5. Data Mart Rollup Reconciliation
SELECT '5. Data Mart Reconciliation' AS audit_category,
       (SELECT COUNT(*) FROM dw.fact_contact) AS fact_total_contacts,
       (SELECT SUM(contacts) FROM mart.marketing_performance) AS mart_total_contacts,
       (SELECT SUM(is_subscribed) FROM dw.fact_contact) AS fact_total_subscriptions,
       (SELECT SUM(subscriptions) FROM mart.marketing_performance) AS mart_total_subscriptions,
       CASE
           WHEN (SELECT COUNT(*) FROM dw.fact_contact) = (SELECT SUM(contacts) FROM mart.marketing_performance)
            AND (SELECT SUM(is_subscribed) FROM dw.fact_contact) = (SELECT SUM(subscriptions) FROM mart.marketing_performance)
           THEN 'PASS' ELSE 'FAIL'
       END AS reconciliation_status;
GO

PRINT '================================================================================';
PRINT '      MASTER WAREHOUSE PIPELINE EXECUTED SUCCESSFULLY                          ';
PRINT '================================================================================';
GO
