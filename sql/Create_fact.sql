USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse (EDW) Layer (Tier 3)
-- Namespace: dw (Clean, indexed dimensional model)
-- Table: dw.fact_contact
--
-- BUSINESS GRANULARITY & ARCHITECTURAL SPECIFICATION:
--   - Fact Table Grain: "one marketing contact/call attempt per customer per campaign event"
--   - Entity Disambiguation:
--       * customer_id: Represents the unique customer entity in dw.dim_customer_profile.
--       * contact_id (fact_contact_sk): Auto-incrementing surrogate primary key 
--         representing the individual event occurrence (call/contact attempt).
--       * Cardinality: 1:N relationship between dw.dim_customer_profile and dw.fact_contact.
--         A single customer can have multiple contact attempts across campaigns.
--   - Elimination of Composite Natural Key Joins:
--       * The fact table stores customer_sk referencing dw.dim_customer_profile(customer_sk).
--       * The 7-column composite join on (age, job, marital, education, default, housing, loan)
--         is completely eliminated.
--       * All dimensional ETL and analytical queries join on a single integer key:
--         fact.customer_sk = dim_customer_profile.customer_sk.
-- ============================================================================
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
    contact_count TINYINT NOT NULL,
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

-- Non-clustered performance indexes on foreign key dimensions (EDW Star Schema Optimization)
CREATE NONCLUSTERED INDEX IX_fact_contact_customer ON dw.fact_contact (customer_sk);
CREATE NONCLUSTERED INDEX IX_fact_contact_month ON dw.fact_contact (month_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_weekday ON dw.fact_contact (weekday_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_channel ON dw.fact_contact (channel_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_outcome ON dw.fact_contact (outcome_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_subscription ON dw.fact_contact (is_subscribed) INCLUDE (contact_count, duration_seconds);
GO

-- ----------------------------------------------------------------------------
-- Fact Population:
-- Eliminates the 7-column composite join anti-pattern by looking up customer_sk
-- directly from dw.dim_customer_profile using the natural key customer_id.
-- ----------------------------------------------------------------------------
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

SELECT 
    COUNT(*) AS total_contacts, 
    COUNT(DISTINCT customer_sk) AS unique_customers_contacted,
    SUM(is_subscribed) AS subscriptions,
    CAST(AVG(CAST(duration_seconds AS FLOAT)) AS DECIMAL(10,2)) AS avg_duration_sec
FROM dw.fact_contact;
GO