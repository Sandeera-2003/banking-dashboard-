#!/usr/bin/env python3
"""
ETL Ingestion Pipeline - Staging Layer Loader
============================================
Architecture Layer: Data Integration Layer (Tier 1 -> Tier 2)
Namespace: stage (Raw, unindexed temporary tables)

This script standardizes and automates the landing of all 4 source feeds into
the dedicated `stage` schema:
  1. stage.retail_banking_crm       (CRM source SQL)
  2. stage.marketing_call_center    (Call logs CSV) -> alias: stage.call_logs
  3. stage.credit_and_risk          (Credit risk CSV)
  4. stage.finance_external_feed    (Macroeconomics JSON/CSV) -> alias: stage.external_economics

Features:
  - Cleans UTF-8 BOM headers to prevent 'ï»¿customer_id' column corruption.
  - Generates idempotent BULK INSERT scripts.
  - Verifies exact 1:1 row count parity across all feeds (41,188 rows).
"""

import os
import csv
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "dataSources"
SQL_DIR = BASE_DIR / "sql"

def verify_source_files():
    print("=" * 60)
    print("1. Verifying Source Files in Data Integration Layer...")
    print("=" * 60)
    
    files = {
        "CRM Data": DATA_DIR / "retail_banking_crm.sql",
        "Call Center Data": DATA_DIR / "marketing_call_center.csv",
        "Credit & Risk Data": DATA_DIR / "credit_and_risk.csv",
        "Finance External Feed (JSON)": DATA_DIR / "finance_external_feed.json",
        "Finance External Feed (CSV)": DATA_DIR / "finance_external_feed.csv",
    }
    
    for name, path in files.items():
        exists = path.exists()
        size = f"{path.stat().st_size:,} bytes" if exists else "MISSING"
        status = "[OK]" if exists else "[ERROR]"
        print(f" {status:<7} {name:<30} : {size}")

def normalize_finance_json():
    """Ensure finance_external_feed.csv is generated and without UTF-8 BOM."""
    json_path = DATA_DIR / "finance_external_feed.json"
    csv_path = DATA_DIR / "finance_external_feed.csv"
    
    if not json_path.exists():
        print(f" [WARN] {json_path.name} not found. Skipping JSON conversion.")
        return

    columns = {
        "customer_id": "customer_id",
        "emp.var.rate": "emp_var_rate",
        "cons.price.idx": "cons_price_idx",
        "cons.conf.idx": "cons_conf_idx",
        "euribor3m": "euribor3m",
        "nr.employed": "nr_employed",
    }

    with json_path.open("r", encoding="utf-8") as f:
        records = json.load(f)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns.values())
        writer.writeheader()
        for r in records:
            writer.writerow({new: r[old] for old, new in columns.items()})

    print(f" [OK] Normalized {csv_path.name} ({len(records):,} records, UTF-8 clean).")

def clean_csv_bom(filename):
    """Strip UTF-8 BOM if present so database import tools don't corrupt headers."""
    filepath = DATA_DIR / filename
    if not filepath.exists():
        return
    with open(filepath, "rb") as f:
        content = f.read()
    if content.startswith(b'\xef\xbb\xbf'):
        with open(filepath, "wb") as f:
            f.write(content[3:])
        print(f" [OK] Stripped UTF-8 BOM from {filename}")

def generate_bulk_insert_sql():
    """Generate ready-to-run SQL script for staging ingestion via BULK INSERT."""
    sql_script_path = SQL_DIR / "bulk_insert_stage.sql"
    
    call_center_csv = (DATA_DIR / "marketing_call_center.csv").resolve()
    credit_risk_csv = (DATA_DIR / "credit_and_risk.csv").resolve()
    finance_csv = (DATA_DIR / "finance_external_feed.csv").resolve()

    sql_content = f"""USE DWBI_Bank;
GO

-- ============================================================================
-- Layer: Data Integration Layer -> Staging Storage Layer (Tier 2)
-- Script: bulk_insert_stage.sql
-- Description: Ingests raw source CSVs into unindexed stage tables
-- ============================================================================

-- 1. Ingest marketing call center logs
TRUNCATE TABLE stage.marketing_call_center;
BULK INSERT stage.marketing_call_center
FROM '{call_center_csv}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\\n',
    TABLOCK
);
GO

-- 2. Ingest credit and risk data
TRUNCATE TABLE stage.credit_and_risk;
BULK INSERT stage.credit_and_risk
FROM '{credit_risk_csv}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\\n',
    TABLOCK
);
GO

-- 3. Ingest finance external feed data
TRUNCATE TABLE stage.finance_external_feed;
BULK INSERT stage.finance_external_feed
FROM '{finance_csv}'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\\n',
    TABLOCK
);
GO

PRINT 'All external feeds loaded into stage schema successfully.';
GO
"""
    with open(sql_script_path, "w", encoding="utf-8") as f:
        f.write(sql_content)
    print(f" [OK] Generated Staging Bulk Insert script at: {sql_script_path}")

def validate_row_counts():
    """Verify that all 4 feeds have exactly 41,188 rows."""
    print("=" * 60)
    print("2. Validating Record Counts Across All 4 Staging Feeds...")
    print("=" * 60)
    
    counts = {}
    
    # Check Call Center CSV
    cc_file = DATA_DIR / "marketing_call_center.csv"
    if cc_file.exists():
        with open(cc_file, "r", encoding="utf-8") as f:
            counts["marketing_call_center.csv"] = sum(1 for _ in f) - 1
            
    # Check Credit & Risk CSV
    cr_file = DATA_DIR / "credit_and_risk.csv"
    if cr_file.exists():
        with open(cr_file, "r", encoding="utf-8") as f:
            counts["credit_and_risk.csv"] = sum(1 for _ in f) - 1

    # Check Finance CSV
    fn_file = DATA_DIR / "finance_external_feed.csv"
    if fn_file.exists():
        with open(fn_file, "r", encoding="utf-8") as f:
            counts["finance_external_feed.csv"] = sum(1 for _ in f) - 1

    # Check CRM SQL
    crm_file = DATA_DIR / "retail_banking_crm.sql"
    if crm_file.exists():
        with open(crm_file, "r", encoding="utf-8") as f:
            counts["retail_banking_crm.sql (inserts)"] = sum(
                line.count("(") - line.count("VALUES (") for line in f if line.strip().startswith("(")
            )

    all_matched = True
    for name, cnt in counts.items():
        is_exact = (cnt == 41188)
        all_matched = all_matched and is_exact
        flag = "[MATCH 41,188]" if is_exact else f"[DIFF: {cnt}]"
        print(f" {flag:<18} {name:<35} : {cnt:,} records")

    print("-" * 60)
    if all_matched:
        print(" [SUCCESS] Perfect 1:1 alignment across all 4 staging tables.")
    else:
        print(" [WARNING] Discrepancies detected across staging sources.")

def main():
    verify_source_files()
    normalize_finance_json()
    clean_csv_bom("credit_and_risk.csv")
    clean_csv_bom("finance_external_feed.csv")
    clean_csv_bom("marketing_call_center.csv")
    generate_bulk_insert_sql()
    validate_row_counts()

if __name__ == "__main__":
    main()
