-- ============================================================================
-- Enterprise Data Architecture: Physical Schema Namespaces
-- System: DWBI_Bank
-- Architecture Pattern: 4-Tier Separation
-- 
-- Namespaces:
--   1. stage : Raw, unindexed, temporary landing tables matching source structures
--   2. edw/dw: Clean, indexed dimensional models (Star Schema dimensions & facts)
--   3. mart  : Aggregated analytical data marts for reporting and BI consumption
-- ============================================================================

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DWBI_Bank')
BEGIN
    CREATE DATABASE DWBI_Bank;
END
GO

USE DWBI_Bank;
GO

-- Namespace 1: Staging Storage Layer (Raw, unindexed, isolated from EDW)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stage')
BEGIN
    EXEC('CREATE SCHEMA stage;');
END
GO

-- Namespace 2: Enterprise Data Warehouse (EDW) Layer (Clean, indexed dimensional models)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
BEGIN
    EXEC('CREATE SCHEMA dw;');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'edw')
BEGIN
    EXEC('CREATE SCHEMA edw;');
END
GO

-- Namespace 3: Analytical Data Marts Layer (Aggregated performance rollups)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'mart')
BEGIN
    EXEC('CREATE SCHEMA mart;');
END
GO