USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Analytical Data Marts Layer (Tier 4)
-- Namespace: mart
-- Table: mart.marketing_performance
-- Description: Aggregated departmental rollup for BI dashboards and reporting
-- Join Pattern: Clean single-integer foreign key join on customer_sk
-- ============================================================================
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

-- Populates aggregated mart joining on single integer key (f.customer_sk = p.customer_sk)
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

SELECT 
    COUNT(*) AS total_mart_rows,
    SUM(contacts) AS contacts, 
    SUM(subscriptions) AS subscriptions,
    SUM(total_duration_seconds) AS total_call_seconds
FROM mart.marketing_performance;
GO