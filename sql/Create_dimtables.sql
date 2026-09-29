USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Enterprise Data Warehouse (EDW) Layer (Tier 3)
-- Namespace: dw (Clean, indexed dimensional model)
-- Table: dw.dim_month
-- ============================================================================
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

(1,'jan','January',1),
(2,'feb','February',1),
(3,'mar','March',1),
(4,'apr','April',2),
(5,'may','May',2),
(6,'jun','June',2),
(7,'jul','July',3),
(8,'aug','August',3),
(9,'sep','September',3),
(10,'oct','October',4),
(11,'nov','November',4),
(12,'dec','December',4);

SELECT * FROM DWBI_Bank.dw.dim_month ORDER BY month_key;