# ETL Migration Notice: Transfer from Python to Pure SQL (T-SQL)

**Branch:** `SQL-Migration`  
**Target Database:** Microsoft SQL Server (`DWBI_Bank`)  
**Status:** Successfully Migrated to Native T-SQL ELT Stored Procedures

---

## 1. Overview

The ETL workflows previously executed via Python (`etl/main_pipeline.py`, `etl/ingest_staging.py`, `etl/json_to_csv.py`) have been transferred to native in-database **T-SQL Stored Procedures** and scripts located in the `sql/` directory.

This architectural shift transitions the data platform from an **ETL (Extract-Transform-Load)** approach to an enterprise **ELT (Extract-Load-Transform)** approach:
- Source data lands directly in `stage.*` unindexed heap tables via `BULK INSERT` and native `OPENJSON`.
- All transformations (cleansing `pdays = 999` to `NULL`, deriving `age_band`, computing `is_subscribed`) and surrogate key assignments execute inside the SQL Server database engine.
- Automated validation checks and assertion gates run natively in T-SQL with execution logs recorded in `etl.pipeline_execution_log`.

---

## 2. Python to SQL Component Mapping

| Python Script / Function | Pure T-SQL Replacement | Location |
| :--- | :--- | :--- |
| `etl/json_to_csv.py` | `etl.usp_03_ingest_external_economics` (via native `OPENJSON`) | [01_stage_ingestion_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/01_stage_ingestion_procedures.sql) |
| `clean_csv_bom()` | `BULK INSERT` with `CODEPAGE = '65001'` (UTF-8) | [01_stage_ingestion_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/01_stage_ingestion_procedures.sql) |
| `etl/ingest_staging.py` | `etl.usp_01_ingest_call_center`, `etl.usp_02_ingest_credit_risk`, `etl.usp_04_verify_staging_parity` | [01_stage_ingestion_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/01_stage_ingestion_procedures.sql) |
| `main_pipeline.py`: Stage 1 (Extract) | `etl.usp_01_*` through `etl.usp_04_*` | [01_stage_ingestion_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/01_stage_ingestion_procedures.sql) |
| `main_pipeline.py`: Stage 2 & 3 (Dimensions) | `etl.usp_10_load_calendar_dimensions`, `etl.usp_11_load_entity_dimensions`, `etl.usp_12_load_customer_profile` | [02_dimensional_loading_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/02_dimensional_loading_procedures.sql) |
| `main_pipeline.py`: Stage 3 (Fact) | `etl.usp_13_load_fact_contact` | [02_dimensional_loading_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/02_dimensional_loading_procedures.sql) |
| `main_pipeline.py`: Stage 3.4 (Data Mart) | `etl.usp_20_load_marketing_performance_mart` | [03_datamart_and_validation_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/03_datamart_and_validation_procedures.sql) |
| `main_pipeline.py`: Stage 4 (Validation) | `etl.usp_30_run_validation_audits` (Criterion 3 Gate) | [03_datamart_and_validation_procedures.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/03_datamart_and_validation_procedures.sql) |
| `main_pipeline.py`: Orchestrator & Logger | `etl.usp_run_master_pipeline` + `etl.pipeline_execution_log` | [04_master_orchestrator.sql](file:///Users/moni/Desktop/DWBI%20proj/etl/04_master_orchestrator.sql) |

---

## 3. How to Run the Pure SQL Pipeline

### Option 1: Orchestrated Stored Procedures (Recommended)
Run the master orchestrator procedure via `sqlcmd` or inside SQL Server Management Studio (SSMS):

```bash
# Provision schemas, logging, and stored procedures (one-time setup)
sqlcmd -S localhost -d DWBI_Bank -i etl/00_setup_schemas_and_logging.sql
sqlcmd -S localhost -d DWBI_Bank -i etl/01_stage_ingestion_procedures.sql
sqlcmd -S localhost -d DWBI_Bank -i etl/02_dimensional_loading_procedures.sql
sqlcmd -S localhost -d DWBI_Bank -i etl/03_datamart_and_validation_procedures.sql
sqlcmd -S localhost -d DWBI_Bank -i etl/04_master_orchestrator.sql

# Execute the complete automated pipeline and view audit report
sqlcmd -S localhost -d DWBI_Bank -i etl/run_pipeline.sql
```

### Option 2: Single Standalone Script
If you prefer a single-script execution without stored procedures:

```bash
sqlcmd -S localhost -d DWBI_Bank -i etl/end_to_end_pipeline.sql
```

---

## 4. Retaining Python Scripts

The original Python scripts ([main_pipeline.py](file:///Users/moni/Desktop/DWBI%20proj/etl/main_pipeline.py), [ingest_staging.py](file:///Users/moni/Desktop/DWBI%20proj/etl/ingest_staging.py), [json_to_csv.py](file:///Users/moni/Desktop/DWBI%20proj/etl/json_to_csv.py)) remain in this directory for historical documentation and cross-platform verification against the local SQLite fallback database (`DWBI_Bank.db`).
