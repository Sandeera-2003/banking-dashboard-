USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Business Intelligence & Multidimensional Analytics (Tier 4)
-- Script: sql/olap_multidimensional_analysis.sql
-- Description: Physical and Analytical Implementation of all 5 OLAP Operations:
--              1. ROLL-UP (Hierarchical Aggregation & Data Mart Summarization)
--              2. DRILL-DOWN (Atomic Grain & Multi-Level Hierarchy Traversal)
--              3. SLICE (Single-Dimensional Sub-Cube Filtering)
--              4. DICE (Multi-Dimensional Attribute Sub-Cube Filtering)
--              5. PIVOT / ROTATE (Cross-Tabulation & Dimensional Re-Orientation)
-- ============================================================================

PRINT '================================================================================';
PRINT '         DWBI_BANK: MULTIDIMENSIONAL OLAP OPERATIONS DEMONSTRATION             ';
PRINT '================================================================================';
GO

-- ----------------------------------------------------------------------------
-- 1. ROLL-UP OPERATION
-- Description: Navigating from finer granularity (atomic fact level) to higher
--              hierarchical tiers (Quarterly, Departmental Mart rollups).
-- ----------------------------------------------------------------------------
PRINT '>>> [OLAP 1: ROLL-UP] Demonstrating Hierarchical Roll-Up by Quarter & Month...';
GO

SELECT 
    m.quarter_no,
    m.month_abbr,
    COUNT(f.contact_id) AS total_contacts,
    SUM(CAST(f.is_subscribed AS INT)) AS subscriptions,
    CAST(ROUND(SUM(CAST(f.is_subscribed AS FLOAT)) * 100.0 / COUNT(f.contact_id), 2) AS DECIMAL(5,2)) AS conversion_rate_pct
FROM dw.fact_contact AS f
JOIN dw.dim_month AS m ON f.month_key = m.month_key
GROUP BY ROLLUP (m.quarter_no, m.month_abbr)
ORDER BY m.quarter_no, m.month_abbr;
GO

-- Data Mart Pre-aggregated Rollup:
PRINT '>>> [OLAP 1b: DATA MART ROLL-UP] Querying mart.marketing_performance...';
GO
SELECT 
    month_key,
    job,
    channel_key,
    contacts,
    subscriptions,
    total_duration_seconds
FROM mart.marketing_performance
ORDER BY contacts DESC;
GO

-- ----------------------------------------------------------------------------
-- 2. DRILL-DOWN OPERATION
-- Description: Navigating from summarized categorical tiers down to finer-grain
--              attributes and atomic customer contact events.
-- ----------------------------------------------------------------------------
PRINT '>>> [OLAP 2: DRILL-DOWN] Demographics Hierarchy (Education -> Job -> Age Band)...';
GO

SELECT 
    p.education,
    p.job,
    p.age_band,
    COUNT(f.contact_id) AS contacts,
    SUM(CAST(f.is_subscribed AS INT)) AS subscriptions,
    CAST(ROUND(SUM(CAST(f.is_subscribed AS FLOAT)) * 100.0 / COUNT(f.contact_id), 2) AS DECIMAL(5,2)) AS conversion_rate_pct
FROM dw.fact_contact AS f
JOIN dw.dim_customer_profile AS p ON f.customer_sk = p.customer_sk
WHERE p.education = 'university.degree'
GROUP BY p.education, p.job, p.age_band
ORDER BY p.education, p.job, p.age_band;
GO

-- Atomic Fact-Grain Drillthrough (Simulating Power BI Page 5 Drillthrough):
PRINT '>>> [OLAP 2b: ATOMIC FACT DRILLTHROUGH] Detailed Customer Event Records...';
GO
SELECT TOP 20
    p.customer_id,
    p.age,
    p.job,
    p.marital,
    p.education,
    ch.contact_type,
    m.month_abbr,
    f.duration_seconds,
    f.is_subscribed
FROM dw.fact_contact AS f
JOIN dw.dim_customer_profile AS p ON f.customer_sk = p.customer_sk
JOIN dw.dim_channel AS ch ON f.channel_key = ch.channel_key
JOIN dw.dim_month AS m ON f.month_key = m.month_key
WHERE p.job = 'technician' AND ch.contact_type = 'cellular';
GO

-- ----------------------------------------------------------------------------
-- 3. SLICE OPERATION
-- Description: Filtering the multidimensional cube along a single dimension member.
--              Example: Analyzing the cellular outreach slice (channel_key = 1).
-- ----------------------------------------------------------------------------
PRINT '>>> [OLAP 3: SLICE] Slicing by Channel = cellular...';
GO

SELECT 
    ch.contact_type,
    COUNT(f.contact_id) AS total_contacts,
    SUM(CAST(f.is_subscribed AS INT)) AS subscriptions,
    CAST(ROUND(SUM(CAST(f.is_subscribed AS FLOAT)) * 100.0 / COUNT(f.contact_id), 2) AS DECIMAL(5,2)) AS conversion_rate_pct,
    AVG(f.duration_seconds) AS avg_call_seconds
FROM dw.fact_contact AS f
JOIN dw.dim_channel AS ch ON f.channel_key = ch.channel_key
WHERE ch.contact_type = 'cellular'
GROUP BY ch.contact_type;
GO

-- ----------------------------------------------------------------------------
-- 4. DICE OPERATION
-- Description: Selecting a multidimensional sub-cube across 3+ orthogonal dimensions:
--              Education = 'university.degree' AND Age Band = '30-39' AND Channel = 'cellular'.
-- ----------------------------------------------------------------------------
PRINT '>>> [OLAP 4: DICE] Dicing across Education, Age Band, and Channel...';
GO

SELECT 
    p.education,
    p.age_band,
    ch.contact_type,
    po.previous_outcome,
    COUNT(f.contact_id) AS contacts,
    SUM(CAST(f.is_subscribed AS INT)) AS subscriptions,
    CAST(ROUND(SUM(CAST(f.is_subscribed AS FLOAT)) * 100.0 / COUNT(f.contact_id), 2) AS DECIMAL(5,2)) AS conversion_rate_pct
FROM dw.fact_contact AS f
JOIN dw.dim_customer_profile AS p ON f.customer_sk = p.customer_sk
JOIN dw.dim_channel AS ch ON f.channel_key = ch.channel_key
JOIN dw.dim_previous_outcome AS po ON f.outcome_key = po.outcome_key
WHERE p.education = 'university.degree'
  AND p.age_band = '30-39'
  AND ch.contact_type = 'cellular'
GROUP BY p.education, p.age_band, ch.contact_type, po.previous_outcome
ORDER BY contacts DESC;
GO

-- ----------------------------------------------------------------------------
-- 5. PIVOT / ROTATE OPERATION
-- Description: Rotating the data axes to present cross-tabulated views of
--              Job Categories (Rows) against Contact Channels (Columns).
-- ----------------------------------------------------------------------------
PRINT '>>> [OLAP 5: PIVOT / ROTATE] Cross-Tabulating Job by Channel...';
GO

SELECT 
    job,
    ISNULL([cellular], 0) AS cellular_contacts,
    ISNULL([telephone], 0) AS telephone_contacts,
    (ISNULL([cellular], 0) + ISNULL([telephone], 0)) AS total_contacts
FROM (
    SELECT 
        p.job,
        ch.contact_type,
        f.contact_id
    FROM dw.fact_contact AS f
    JOIN dw.dim_customer_profile AS p ON f.customer_sk = p.customer_sk
    JOIN dw.dim_channel AS ch ON f.channel_key = ch.channel_key
) AS SourceTable
PIVOT (
    COUNT(contact_id)
    FOR contact_type IN ([cellular], [telephone])
) AS PivotTable
ORDER BY total_contacts DESC;
GO

PRINT '================================================================================';
PRINT '         OLAP OPERATIONS VERIFICATION COMPLETED SUCCESSFULLY                   ';
PRINT '================================================================================';
GO
