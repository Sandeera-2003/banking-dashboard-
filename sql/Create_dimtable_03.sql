USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse (EDW) Layer (Tier 3)
-- Namespace: dw (Clean, indexed dimensional model)
-- Table: dw.dim_customer_profile
-- Source: stage.retail_banking_crm & stage.credit_and_risk
-- ============================================================================
IF OBJECT_ID('dw.dim_customer_profile', 'U') IS NOT NULL
    DROP TABLE dw.dim_customer_profile;
GO

CREATE TABLE dw.dim_customer_profile (
    profile_key INT IDENTITY(1,1) PRIMARY KEY,
    age INT NOT NULL,
    age_band VARCHAR(12) NOT NULL,
    job VARCHAR(50),
    marital VARCHAR(20),
    education VARCHAR(30),
    default_status VARCHAR(10),
    housing_loan VARCHAR(10),
    personal_loan VARCHAR(10)
);
GO

INSERT INTO dw.dim_customer_profile
    (age, age_band, job, marital, education, default_status, housing_loan, personal_loan)
SELECT DISTINCT
    c.age,
    CASE
        WHEN c.age < 30 THEN 'Under 30'
        WHEN c.age < 40 THEN '30-39'
        WHEN c.age < 50 THEN '40-49'
        WHEN c.age < 60 THEN '50-59'
        ELSE '60 and over'
    END,
    c.job, c.marital, c.education,
    r.[default], r.housing, r.loan
FROM stage.retail_banking_crm AS c
JOIN stage.credit_and_risk AS r ON c.customer_id = r.customer_id;
GO

SELECT COUNT(*) AS profiles FROM dw.dim_customer_profile;