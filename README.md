# Retail Orders Data Quality Audit

## Overview
This repository contains scripts developed to perform a comprehensive **data quality audit** on retail order datasets. The primary goal is to identify inconsistencies, duplicates, missing values, and other anomalies in order data, and generate actionable reports for data cleaning and validation.

The scripts were designed to handle **CSV** and **Excel** files with multiple sheets and varying data quality issues.

---

## Project Scope
Key data quality checks performed include:

1. **Duplicate Identification**
   - Detect exact duplicates based on key columns (`order_id`, `product_id`).
   - Identify “special duplicates” where minor discrepancies exist.

2. **Missing Values**
   - Validate mandatory columns for missing entries.
   - Pad postal codes and standardize formats where necessary.

3. **Logical Validation**
   - Check for negative quantities.
   - Validate relationships between `quantity`, `sales`, and `profit`.
   - Verify `order_date` vs `ship_date` correctness.

4. **Categorical & Reference Checks**
   - Detect unexpected categories in product or order fields.
   - Validate postal codes and other reference values.

5. **Inconsistency Analysis**
   - Flag rows with multiple issues for review.
   - Track which columns contributed to inconsistencies.

6. **Reporting**
   - Generate **summary reports** with counts and percentages of inconsistencies.
   - Create **example reports** showing sample invalid rows.
   - Provide **quality dashboards** including charts for visualization.

---


