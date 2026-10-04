USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Operational & Audit Layer (Tier 0)
-- Script: 00_setup_schemas_and_logging.sql
-- Description: Provisions enterprise schema namespaces (stage, dw, edw, mart, etl)
--              and establishes the ETL logging and audit trail infrastructure.
-- Criteria Fulfillment: Criterion 3 (Enterprise Automated Orchestration & Audits)
-- ============================================================================

PRINT '================================================================================';
PRINT '  [STEP 0] PROVISIONING SCHEMA NAMESPACES & AUDIT INFRASTRUCTURE                ';
PRINT '================================================================================';
GO

-- 1. Provision Physical Schemas
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
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'etl')
    EXEC('CREATE SCHEMA etl;');
GO

PRINT '  [OK] Physical Schemas (stage, dw, edw, mart, etl) verified.';
GO

-- 2. Pipeline Configuration Table
IF OBJECT_ID('etl.pipeline_config', 'U') IS NOT NULL
    DROP TABLE etl.pipeline_config;
GO

CREATE TABLE etl.pipeline_config (
    config_key VARCHAR(100) PRIMARY KEY,
    config_value NVARCHAR(500) NOT NULL,
    description NVARCHAR(500) NULL,
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);
GO

-- Seed default datasource path configuration (can be updated dynamically)
INSERT INTO etl.pipeline_config (config_key, config_value, description)
VALUES 
    ('DATASOURCES_PATH', '/Users/moni/Desktop/DWBI proj/dataSources', 'Base directory containing raw CSV, JSON, and SQL feeds'),
    ('EXPECTED_STAGING_ROWS', '41188', 'Expected row count benchmark per source feed'),
    ('EXPECTED_CLEANSED_PDAYS', '39673', 'Expected pdays sentinel count cleansed to NULL'),
    ('EXPECTED_SUBSCRIPTIONS', '4640', 'Expected positive subscription outcome count');
GO

PRINT '  [OK] etl.pipeline_config created and seeded.';
GO

-- 3. Pipeline Execution Run Log Table
IF OBJECT_ID('etl.pipeline_execution_log', 'U') IS NOT NULL
    DROP TABLE etl.pipeline_execution_log;
GO

CREATE TABLE etl.pipeline_execution_log (
    log_id INT IDENTITY(1,1) PRIMARY KEY,
    execution_id UNIQUEIDENTIFIER NOT NULL,
    step_number INT NOT NULL,
    step_name VARCHAR(100) NOT NULL,
    start_time DATETIME2 NOT NULL,
    end_time DATETIME2 NULL,
    duration_seconds DECIMAL(9,2) NULL,
    rows_affected INT NULL,
    status VARCHAR(20) NOT NULL, -- 'RUNNING', 'SUCCESS', 'FAILED'
    error_message NVARCHAR(MAX) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_pipeline_log_exec ON etl.pipeline_execution_log (execution_id, step_number);
GO

PRINT '  [OK] etl.pipeline_execution_log table provisioned.';
GO

-- 4. Pipeline Validation Audit Log Table (Criterion 3 Evidence)
IF OBJECT_ID('etl.pipeline_validation_audit', 'U') IS NOT NULL
    DROP TABLE etl.pipeline_validation_audit;
GO

CREATE TABLE etl.pipeline_validation_audit (
    audit_id INT IDENTITY(1,1) PRIMARY KEY,
    execution_id UNIQUEIDENTIFIER NOT NULL,
    audit_timestamp DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    audit_category VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    actual_value NVARCHAR(100) NOT NULL,
    expected_value NVARCHAR(100) NOT NULL,
    audit_status VARCHAR(10) NOT NULL, -- 'PASS', 'FAIL'
    notes NVARCHAR(500) NULL
);
GO

CREATE NONCLUSTERED INDEX IX_pipeline_audit_exec ON etl.pipeline_validation_audit (execution_id, audit_category);
GO

PRINT '  [OK] etl.pipeline_validation_audit table provisioned.';
GO

PRINT '--------------------------------------------------------------------------------';
PRINT '  [STEP 0 COMPLETE] Enterprise schemas & audit infrastructure ready.';
PRINT '--------------------------------------------------------------------------------';
GO
