USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse (EDW) Layer (Tier 3)
-- Namespace: dw (Clean, indexed dimensional model)
-- Table: dw.fact_contact
-- Description: Central fact table recording marketing telemarketing contacts
-- ============================================================================
IF OBJECT_ID('dw.fact_contact', 'U') IS NOT NULL
    DROP TABLE dw.fact_contact;
GO

CREATE TABLE dw.fact_contact (
    contact_id INT PRIMARY KEY,
    profile_key INT NOT NULL,
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

    FOREIGN KEY (profile_key) REFERENCES dw.dim_customer_profile(profile_key),
    FOREIGN KEY (month_key) REFERENCES dw.dim_month(month_key),
    FOREIGN KEY (weekday_key) REFERENCES dw.dim_weekday(weekday_key),
    FOREIGN KEY (channel_key) REFERENCES dw.dim_channel(channel_key),
    FOREIGN KEY (outcome_key) REFERENCES dw.dim_previous_outcome(outcome_key)
);
GO

-- Non-clustered performance indexes on foreign key dimensions (EDW Star Schema Optimization)
CREATE NONCLUSTERED INDEX IX_fact_contact_profile ON dw.fact_contact (profile_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_month ON dw.fact_contact (month_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_weekday ON dw.fact_contact (weekday_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_channel ON dw.fact_contact (channel_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_outcome ON dw.fact_contact (outcome_key);
CREATE NONCLUSTERED INDEX IX_fact_contact_subscription ON dw.fact_contact (is_subscribed) INCLUDE (contact_count, duration_seconds);
GO

INSERT INTO dw.fact_contact (
    contact_id, profile_key, month_key, weekday_key, channel_key,
    outcome_key, contact_count, is_subscribed, duration_seconds,
    campaign_contacts, previous_contacts, days_since_previous,
    emp_var_rate, cons_price_idx, cons_conf_idx, euribor3m, nr_employed
)
SELECT
    m.customer_id,
    p.profile_key,
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
JOIN stage.retail_banking_crm AS c
    ON m.customer_id = c.customer_id
JOIN stage.credit_and_risk AS r
    ON m.customer_id = r.customer_id
JOIN stage.finance_external_feed AS e
    ON m.customer_id = e.customer_id
JOIN dw.dim_customer_profile AS p
    ON p.age = c.age
    AND p.job = c.job
    AND p.marital = c.marital
    AND p.education = c.education
    AND p.default_status = r.[default]
    AND p.housing_loan = r.housing
    AND p.personal_loan = r.loan
JOIN dw.dim_month AS mo
    ON mo.month_abbr = m.[month]
JOIN dw.dim_weekday AS wd
    ON wd.day_abbr = m.day_of_week
JOIN dw.dim_channel AS ch
    ON ch.contact_type = m.contact
JOIN dw.dim_previous_outcome AS po
    ON po.previous_outcome = m.poutcome;
GO

SELECT COUNT(*) AS contacts, SUM(is_subscribed) AS subscriptions FROM DWBI_Bank.dw.fact_contact;