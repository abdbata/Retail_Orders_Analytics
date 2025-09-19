# Retail Orders Data Quality Audit


## Overview

This ETL pipeline automates the entire data ingestion and preparation process for retail order datasets. It supports:
- Downloading files from SFTP servers (.zip, .rar, .csv, .xlsx)
- Extracting compressed archives
- Merging multiple CSV/XLSX files
- Data quality auditing (duplicates, missing values, negative sales, date validation, category checks, postal codes, etc.)
- Cleaning and fixing inconsistencies
- Loading data into PostgreSQL staging, master, and fact tables
- Creating data marts for reporting in Power BI

---

## Features

- SFTP download automation
- Archive extraction (ZIP/RAR)
- Data quality validation & automated fixes
- Detailed reporting of inconsistencies
- Cleaned dataset export
- PostgreSQL staging, master dimension, and fact table population
- Ready-to-use data marts for Power BI dashboards


## Architecture & Process

SFTP Server (.zip/.csv/.xlsx)
          │
          ▼
Download files (Python/Paramiko)
          │
          ▼
Extract archives (ZIP/RAR)
          │
          ▼
Merge all CSV/XLSX
          │
          ▼
Data Quality Audit (duplicates, missing, negatives, dates, categories, postal codes)
          │
          ├── Generate Excel Report
          │     - Summary
          │     - Examples
          │     - Quality Charts
          │
          ▼
Clean dataset (remove/fix invalid rows)
          │
          ▼
Load to PostgreSQL
          ├── Staging (stg.stg_orders_raw)
          ├── Master Dimensions (mdm)
          │     - Customer
          │     - Product
          │     - Geography
          └── Fact Table (dwh.dwh_fact_sales_order)
          │
          ▼
Build Data Marts (dm schema)
          - Sales by State & Month
          - Best Customers by Segment
          - Top/Bottom Products per State
          │
          ▼
Power BI / Visualization


## ETL & Data Quality Process

1️⃣ SFTP Download
Connect to SFTP using Paramiko
Download .zip, .rar, .csv, .xlsx files

2️⃣ Archive Extraction
Extract ZIP using zipfile
Extract RAR using patoolib

3️⃣ Merge CSV/XLSX Files
Standardize column names
Append all data into a single DataFrame
Add source_file column

4️⃣ Data Quality Audit
Check for duplicate orders (order_id, product_id)
Check for special duplicates (order_id, product_id, sales)
Detect missing mandatory columns
Detect negative sales or profits
Check quantity-profit logic & quantity-sales logic
Validate date format (DD-MM-YYYY)
Ensure order_date ≤ ship_date
Validate category and postal code
Generate summary, examples, and quality charts

5️⃣ Dataset Cleaning
Remove/fix inconsistent rows based on audit
Fix dates and postal codes
Save cleaned dataset and dropped rows

6️⃣ Load to PostgreSQL
Ensure schemas: stg, mdm, dwh, dm
Load cleaned data to staging (stg.stg_orders_raw)
Populate master tables (mdm)
Populate fact table (dwh.dwh_fact_sales_order)

7️⃣ Build Data Marts (dm schema)
Sales by state & month
Best customers per segment
Top/bottom products by state


## Database Schema (PostgreSQL)
- Staging Table (stg.stg_orders_raw): raw cleaned orders
- Master Dimensions (mdm):
   - mdm_dim_customer
   - mdm_dim_product
   - mdm_dim_geography
- Fact Table (dwh.dwh_fact_sales_order): sales order fact table
- Data Marts (dm): ready for reporting

## Data Marts & Power BI
Data marts are built for Power BI dashboards:
- Sales trends by state/month
- Top 10 customers per segment
- Top & bottom products per state
Power BI can connect directly to the dm schema to visualize insights.

## Dependencies
Python 3.9+
pandas
openpyxl
paramiko
patoolib
psycopg2-binary
SQLAlchemy
matplotlib
Tkinter (standard)



