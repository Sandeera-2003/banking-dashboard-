USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse (EDW) Layer (Tier 3)
-- Namespace: dw (Clean, indexed dimensional models)
-- Tables: dw.dim_channel, dw.dim_weekday, dw.dim_previous_outcome
-- Source: stage.marketing_call_center
-- ============================================================================

-- 1. Dimension: Channel
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

-- 2. Dimension: Weekday
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
(1,'mon','Monday'),
(2,'tue','Tuesday'),
(3,'wed','Wednesday'),
(4,'thu','Thursday'),
(5,'fri','Friday');
GO

-- 3. Dimension: Previous Campaign Outcome
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

-- 4. Dimension: Campaign Contacts & Band (Entity Dimension)
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

SELECT 
    (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_channel) AS channels, 
    (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_weekday) AS weekdays, 
    (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_previous_outcome) AS previous_outcomes,
    (SELECT COUNT(*) FROM DWBI_Bank.dw.dim_campaign) AS campaigns;
GO