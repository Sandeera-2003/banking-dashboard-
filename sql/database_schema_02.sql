USE DWBI_Bank;
GO

-- ============================================================================
-- Validation Script: Staging Layer Row Counts & Referential Integrity
-- Layer: Staging Storage Layer (Tier 2 - stage schema)
-- Verifies that all 4 source feeds landed with 41,188 rows and zero missing keys
-- ============================================================================

-- 1. Validate CRM Staging Table (Raw demographics)
SELECT 'stage.retail_banking_crm' AS table_name,
       COUNT(*) AS total_rows, 
       MIN(customer_id) AS first_id, 
       MAX(customer_id) AS last_id 
FROM DWBI_Bank.stage.retail_banking_crm;

-- 2. Validate Marketing Call Center Staging Table (Raw call logs)
SELECT 'stage.marketing_call_center' AS table_name,
       COUNT(*) AS total_rows, 
       SUM(CASE WHEN y = 'yes' THEN 1 ELSE 0 END) AS subscriptions 
FROM DWBI_Bank.stage.marketing_call_center;

-- 3. Validate Credit & Risk Staging Table (Raw financial status)
SELECT 'stage.credit_and_risk' AS table_name,
       COUNT(*) AS total_rows, 
       COUNT(DISTINCT customer_id) AS unique_ids 
FROM DWBI_Bank.stage.credit_and_risk;

-- 4. Validate Finance External Feed Staging Table (Raw economic indicators)
SELECT 'stage.finance_external_feed' AS table_name,
       COUNT(*) AS total_rows, 
       COUNT(DISTINCT customer_id) AS unique_ids 
FROM DWBI_Bank.stage.finance_external_feed;

-- 5. Cross-Feed Staging Integrity Join Check (Verifies 100% 1:1 key parity)
SELECT 
    COUNT(*) AS joined_rows,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_crm,
    SUM(CASE WHEN r.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_credit,
    SUM(CASE WHEN e.customer_id IS NULL THEN 1 ELSE 0 END) AS missing_economic,
    SUM(CASE WHEN m.y = 'yes' THEN 1 ELSE 0 END) AS subscriptions
FROM DWBI_Bank.stage.marketing_call_center AS m
LEFT JOIN DWBI_Bank.stage.retail_banking_crm AS c ON m.customer_id = c.customer_id
LEFT JOIN DWBI_Bank.stage.credit_and_risk AS r ON m.customer_id = r.customer_id
LEFT JOIN DWBI_Bank.stage.finance_external_feed AS e ON m.customer_id = e.customer_id;
GO