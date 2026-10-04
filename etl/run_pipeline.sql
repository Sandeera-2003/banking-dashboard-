USE DWBI_Bank;
GO

-- ============================================================================
-- Script: run_pipeline.sql
-- Description: Execution runner script to invoke the master T-SQL pipeline
--              orchestrator and display execution and validation audit logs.
-- Usage:
--   sqlcmd -S localhost -d DWBI_Bank -i etl/run_pipeline.sql
-- ============================================================================

PRINT 'Starting DWBI Bank Master Pipeline Execution...';
GO

EXEC etl.usp_run_master_pipeline;
GO

-- Display Execution Log Summary
PRINT '';
PRINT '================================================================================';
PRINT '  EXECUTION LOG SUMMARY (Latest Batch)';
PRINT '================================================================================';
SELECT TOP 10
    step_number,
    step_name,
    duration_seconds,
    rows_affected,
    status,
    CONVERT(VARCHAR, start_time, 120) AS started_at,
    error_message
FROM etl.pipeline_execution_log
ORDER BY log_id DESC;
GO

-- Display Validation Audit Results
PRINT '';
PRINT '================================================================================';
PRINT '  VALIDATION AUDIT SUMMARY (Criterion 3 Evidence)';
PRINT '================================================================================';
SELECT TOP 16
    audit_category,
    metric_name,
    actual_value,
    expected_value,
    audit_status,
    notes
FROM etl.pipeline_validation_audit
ORDER BY audit_id DESC;
GO
