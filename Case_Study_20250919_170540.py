import tkinter as tk
import zipfile
import shutil
import os
from datetime import datetime
from tkinter import filedialog, messagebox
import paramiko
import os
import zipfile
import patoolib
import pandas as pd
from openpyxl import Workbook
import psycopg2
from sqlalchemy import create_engine, text
from datetime import datetime
import matplotlib.pyplot as plt
from datetime import datetime
import os
import pandas as pd
from tkinter import messagebox
from openpyxl.styles import Font
import os
import pandas as pd
from datetime import datetime
from tkinter import messagebox
from openpyxl.styles import Font
import traceback
from sqlalchemy import text

# ---------- POSTGRESQL CONFIG ----------
PG_CONN = "postgresql+psycopg2://postgres:Nokia_123@localhost:5432/retail_db1"
engine = create_engine(PG_CONN)

# ---------- SFTP CONFIG ----------
SFTP_HOST = "localhost"
SFTP_PORT = 22
SFTP_USERNAME = "abedalmenaam"
SFTP_PASSWORD = "Bata@1234+-"
SFTP_REMOTE_FOLDER = "SFTP_TestData"  # inside Windows home
MERGED_CSV_FILE = "Merged_Data.xlsx"
Task_5_REPORT_FILE = "Task_5_Inconsistencies_Analysis_"

# ---------- DATA AUDIT CONFIGURATION ----------

ALL_COLUMNS = [
    'row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode',
    'customer_id', 'customer_name', 'segment', 'country', 'city',
    'state', 'postal_code', 'region', 'product_id', 'category',
    'sub_category', 'product_name', 'sales', 'quantity', 'discount', 'profit']


# ---------- Configurable columns ----------
DATE_COLUMNS = ['order_date', 'ship_date']
MANDATORY_COLUMNS = ['order_id', 'customer_id', 'product_id', 'quantity', 'sales', 'order_date', 'ship_date']
NEGATIVE_COLUMNS = ['sales', 'profit', 'quantity']
CATEGORY_COLUMNS = ['category']
POSTAL_CODE_COLUMN = 'postal_code'

# ---------- GUI FUNCTIONS ----------
def select_save_path():
    path = filedialog.askdirectory()
    save_path_var.set(path)

def download_files_from_sftp(save_path):
    """Download .zip, .rar, .csv, .xlsx files from SFTP to the given folder."""
    downloaded_files = []
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.chdir(SFTP_REMOTE_FOLDER)
        files = sftp.listdir()

        for file in files:
            if file.lower().endswith((".zip", ".rar", ".csv", ".xlsx")):
                local_file_path = os.path.join(save_path, file)
                try:
                    sftp.get(file, local_file_path)
                    downloaded_files.append(file)
                except Exception as e:
                    messagebox.showwarning("Warning", f"Failed to download {file}: {e}")

        sftp.close()
        transport.close()

        if not downloaded_files:
            messagebox.showinfo("Info", "No files downloaded.")
            return []

        return downloaded_files

    except Exception as e:
        messagebox.showerror("Error", f"Unexpected error while downloading:\n{e}")
        return []


def extract_archives(folder):
    """Recursively extract ZIP and RAR files."""
    for root_dir, _, files in os.walk(folder):
        for f in files:
            file_path = os.path.join(root_dir, f)
            try:
                if f.lower().endswith(".zip"):
                    with zipfile.ZipFile(file_path, 'r') as zip_ref:
                        zip_ref.extractall(root_dir)
                elif f.lower().endswith(".rar"):
                    patoolib.extract_archive(file_path, outdir=root_dir)
            except Exception as e:
                messagebox.showwarning("Warning", f"Failed to extract {f}: {e}")


def merge_all_csv_files(folder):
    print("Merging all csv/xlsx files into one table.")
    all_data = []

    # Get all CSV/XLSX files
    files = [os.path.join(root, f) for root, _, fs in os.walk(folder)
             for f in fs if f.lower().endswith(('.csv', '.xlsx'))]

    if not files:
        messagebox.showinfo("Info", "No CSV/XLSX files to process.")
        return None

    # --------- PROCESS FILES ---------
    for file_path in files:
        f = os.path.basename(file_path)
        inconsistent_indices = set()

        try:
            if f.lower().endswith('.csv'):
                df = pd.read_csv(
                    file_path,
                    sep='|',               # pipe delimiter
                    encoding='ANSI',       # your dataset encoding
                    keep_default_na=True,
                    na_values=['', ' ']
                )
            else:
                df = pd.read_excel(
                    file_path,
                    na_values=['', ' ']
                )
        except Exception as e:
            messagebox.showwarning("Warning", f"Failed to read {f}: {e}")
            continue

        # Normalize column names
        df = df.reset_index(drop=True)
        df.columns = [col.strip().lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Add a column to track source file
        df['source_file'] = f

        all_data.append(df)

    # --------- MERGE & SAVE ---------
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        merged_path = os.path.join(folder, MERGED_CSV_FILE)
        merged_df.to_excel(merged_path, index=False)
        print("✅ Merged file saved at:", merged_path)
        return merged_df  
    else:
        messagebox.showinfo("Info", "No data merged.")
        return None


def scan_inconsistencies(save_path, merged_df):
    """
    Scan merged_df for inconsistencies, generate multi-sheet report, and save charts.
    """
    if merged_df is None or merged_df.empty:
        messagebox.showinfo("Info", "No data to scan for inconsistencies.")
        return None

    df = merged_df.copy()
    summary_records = []
    examples_records = []
    quality_records = []

    # Initialize tracking
    df['Inconsistent_Cols'] = [[] for _ in range(len(df))]
    df['Inconsistency_Type'] = [[] for _ in range(len(df))]


    # ---------- Duplicate Check ----------
    # Mark only extra duplicates (not the first occurrence)
    duplicates = df.duplicated(subset=['order_id', 'product_id'], keep='first')

    df['Duplicate_Order_Product_Check'] = duplicates
    df['Duplicate_Order_Product_Solution'] = duplicates.apply(lambda x: "Remove_Duplicate_Rows" if x else "")
    for idx in df[duplicates].index:  # only the extra ones
        df.at[idx, 'Inconsistent_Cols'].append('order_id/product_id')
        df.at[idx, 'Inconsistency_Type'].append('Duplicate Rows')



    # ---------- Special Duplicate Check ----------
    # Check across the whole dataframe (do not exclude duplicates)
    special_duplicates = df.duplicated(subset=['order_id', 'product_id', 'sales'], keep='first')

    df['Special_Duplicate_Check'] = False  # initialize
    df.loc[special_duplicates.index, 'Special_Duplicate_Check'] = True

    df['Special_Duplicate_Solution'] = df['Special_Duplicate_Check'].apply(
        lambda x: "Remove_Special_Duplicate_Rows" if x else ""
    )

    for idx in special_duplicates[special_duplicates].index:  # only the extra ones
        df.at[idx, 'Inconsistent_Cols'].append('order_id/product_id/sales')
        df.at[idx, 'Inconsistency_Type'].append('Special Duplicates')



    # ---------- Missing Values ----------
    missing_mask = df[MANDATORY_COLUMNS].isna().any(axis=1)
    df['Missing_Values_Check'] = missing_mask
    for idx in df[missing_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('mandatory_columns')
        df.at[idx, 'Inconsistency_Type'].append('Missing Values')

    # ---------- Negative Check ----------
    negative_mask = (df['sales'] < 0)
    df['Negative_Check'] = negative_mask
    for idx in df[negative_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('sales')
        df.at[idx, 'Inconsistency_Type'].append('Negative quantitys')

    # ---------- Quantity-Profit Logic ----------
    quantity_profit_mask = ((df['quantity'] < 0) & (df['profit'] > 0))
    df['Quantity_Profit_Check'] = quantity_profit_mask
    for idx in df[quantity_profit_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('quantity/profit')
        df.at[idx, 'Inconsistency_Type'].append('Quantity-Profit Logic')

    # ---------- Quantity-Sales Logic ----------
    quantity_sales_mask = (df['quantity'] == 0) & (df['sales'] > 0)
    df['quantity_Sales_Check'] = quantity_sales_mask
    for idx in df[quantity_sales_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('quantity/sales')
        df.at[idx, 'Inconsistency_Type'].append('quantity-Sales Logic')

    # ---------- Date Format Check ----------
    def validate_and_fix_date(date_str):
        try:
            dt = datetime.strptime(str(date_str), "%d-%m-%Y")
            return True, dt.strftime("%d-%m-%Y")
        except:
            return False, None

    for col in DATE_COLUMNS:
        check_results = df[col].apply(validate_and_fix_date)
        df[f'{col}_Date_Format_Check'] = check_results.apply(lambda x: not x[0])
        df[f'{col}_Date_Format_Solution'] = check_results.apply(lambda x: x[1] if x[0] else None)
        for idx, (is_valid, fixed_date) in enumerate(check_results):
            if not is_valid:
                df.at[idx, 'Inconsistent_Cols'].append(col)
                df.at[idx, 'Inconsistency_Type'].append('Invalid Dates')
                df.at[idx, f'{col}_Date_Format_Solution'] = fixed_date


    # ---------- Order Date vs Ship Date ----------
    order_dt = pd.to_datetime(df['order_date'], dayfirst=True, errors='coerce')
    ship_dt = pd.to_datetime(df['ship_date'], dayfirst=True, errors='coerce')
    order_after_ship_mask = order_dt > ship_dt
    df['OrderDate_After_DeliveredDate_Check'] = order_after_ship_mask
    for idx in df[order_after_ship_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('order_date/ship_date')
        df.at[idx, 'Inconsistency_Type'].append('Order Date After Delivery Date')

    # ---------- Unexpected Category ----------
    valid_categories = ['Office Supplies', 'Furniture', 'Technology']
    category_mask = ~df['category'].isin(valid_categories)
    df['Unexpected_Category_Check'] = category_mask
    df['Unexpected_Category_Solution'] = df['category'].apply(lambda x: x if x in valid_categories else 'Check with Customer')
    for idx in df[category_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('category')
        df.at[idx, 'Inconsistency_Type'].append('Unexpected Category')

    # ---------- Postal Code Check & Solution ----------
    POSTAL_CODE_COLUMN = 'postal_code'
    # Boolean mask: True if postal code is not 5 digits
    postal_mask = df[POSTAL_CODE_COLUMN].apply(lambda x: len(str(x)) != 5)
    # Solution column: pad with leading zeros if needed
    df['Postal_Code_Solution'] = df[POSTAL_CODE_COLUMN].apply(lambda x: str(x).zfill(5) if len(str(x)) != 5 else str(x))
    # Mark inconsistent rows for reporting
    for idx in df[postal_mask].index:
        df.at[idx, 'Inconsistent_Cols'].append('postal_code')
        df.at[idx, 'Inconsistency_Type'].append('Postal Code Length')


    # ---------- Quality Records & Summary ----------
    source_files = df['source_file'].unique()
    # Map inconsistency types to suggested actions
    suggestion_map = {
        'Duplicate Rows': "Remove Duplicate Rows",
        'Missing Values': "Keep but share with customer to fill if available",
        'Negative quantitys': "Remove Row and share with customer",
        'Quantity-Profit Logic': "Remove Row and share with customer",
        'quantity-Sales Logic': "Remove Row and share with customer",
        'Invalid Dates': "Fix Format",
        'Order Date After Delivery Date': "Remove Row and share with customer",
        'Unexpected Category': "Remove Row and share with customer",
        'Postal Code Length': "Fix Format",
        'Special Duplicates': "Remove Duplicate Row and share with customer"
    }
    description_map = {
    'Duplicate Rows': "Extra duplicate rows based on order_id and product_id",
    'Special Duplicates': "Duplicate rows considering order_id, product_id, and sales",
    'Missing Values': "Mandatory columns missing values",
    'Negative quantitys': "Sales quantity is negative",
    'Quantity-Profit Logic': "Quantity negative while profit positive",
    'quantity-Sales Logic': "Quantity is zero but sales > 0",
    'Invalid Dates': "Dates are not in correct format (DD-MM-YYYY)",
    'Order Date After Delivery Date': "Order date occurs after ship/delivery date",
    'Unexpected Category': "Category is not in the expected set",
    'Postal Code Length': "Postal code is not 5 digits"
    }



    for f in source_files:
        sub_df = df[df['source_file'] == f]
        total_rows = sub_df.shape[0]

        invalid_removed = 0
        invalid_fixed = 0

        # Count inconsistencies based on Inconsistency_Type
        for idx, inc_types in sub_df['Inconsistency_Type'].items():
            if not inc_types:
                continue

            # Map to suggestions
            row_suggestions = [suggestion_map.get(inc, "") for inc in inc_types]

            if any("Remove" in s for s in row_suggestions):
                invalid_removed += 1
            elif any("Fix" in s or "Keep" in s for s in row_suggestions):
                invalid_fixed += 1

        valid_rows = max(0, total_rows - (invalid_removed + invalid_fixed))
        validation_percent = round(valid_rows / total_rows * 100, 2) if total_rows > 0 else 0

        quality_records.append({
            'Source File': f,
            'Total Rows': total_rows,
            'Valid Rows': valid_rows,
            'Invalid Rows': invalid_removed + invalid_fixed,
            'Invalid Removed Rows': invalid_removed,
            'Invalid Fixed Rows': invalid_fixed,
            'Validation %': validation_percent
        })

        # Build summary with proper suggestions
        for inc_type in sub_df['Inconsistency_Type'].explode().dropna().unique():
            distinct_count = sub_df[sub_df['Inconsistency_Type'].apply(lambda x: inc_type in x)].shape[0]
            summary_records.append({
                'Inconsistency Type': inc_type,
                'Description': description_map.get(inc_type, "Check and validate"),
                'Suggestion to Handle': suggestion_map.get(inc_type, "Check with Customer"),
                'Distinct Count of Rows': distinct_count,
                'Source File': f
            })

    # ---------- Examples: max 2 rows per type globally ----------
    df_exploded = df.explode('Inconsistency_Type')
    examples_records = []

    for inc_type in df_exploded['Inconsistency_Type'].dropna().unique():
        examples = df_exploded[df_exploded['Inconsistency_Type'] == inc_type].head(2)
        examples_records.append(examples)

    examples_df = pd.concat(examples_records) if examples_records else pd.DataFrame()

    # ---------- Save to Excel ----------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    report_file_with_time = os.path.join(save_path, f"{Task_5_REPORT_FILE}_{timestamp}.xlsx")

    examples_cols = [
        'row_id','order_id','order_date','ship_date','ship_mode',
        'customer_id','customer_name','segment','country','city','state','postal_code','region',
        'product_id','category','sub_category','product_name','sales','quantity','discount','profit','source_file',
        'Inconsistency_Type','Inconsistent_Cols'    ]

    examples_df_to_save = examples_df[examples_cols].copy()
    examples_df_to_save['_inconsistent_cols'] = examples_df_to_save['Inconsistent_Cols']

    with pd.ExcelWriter(report_file_with_time, engine='openpyxl') as writer:
        pd.DataFrame(summary_records).to_excel(writer, sheet_name='Inconsistencies_Summary', index=False)
        examples_df_to_save.to_excel(writer, sheet_name='Inconsistencies_Examples', index=False)
        pd.DataFrame(quality_records).to_excel(writer, sheet_name='Quality_Report', index=False)

    print("✅ Inconsistencies_Examples saved at:", report_file_with_time)

    # ---------- Save full analyzed DataFrame ----------
    AUDIT_CHECK_COLUMNS = [
        'Duplicate_Order_ID_Check', 'Duplicate_Order_ID_Solution',
        'Missing_Values_Check', 'Negative_Check', 'quantity_Sales_Check',
        'order_date_Date_Format_Check', 'ship_date_Date_Format_Check',
        'OrderDate_After_DeliveredDate_Check',
        'Unexpected_Category_Check', 'Unexpected_Category_Solution',
        'Postal_Code_Check'
    ]
    solution_cols = [f"{col}_Date_Format_Solution" for col in DATE_COLUMNS] + ['Postal_Code_Solution']
    cols_to_save = [c for c in df.columns if c not in AUDIT_CHECK_COLUMNS] + solution_cols
    df_to_save = df[cols_to_save]

    inconsistency_analysis_file = os.path.join(save_path, f"Inconsistencies_Analysis_{timestamp}.xlsx")
    df_to_save.to_excel(inconsistency_analysis_file, index=False)
    print("✅ Inconsistencies_Analysis saved at:", inconsistency_analysis_file)



    # ---------- Visualize Quality Data ----------
    etl_folder = save_path  # or wherever you want to save charts
    if quality_records:
        quality_df = pd.DataFrame(quality_records)
        valid_total = quality_df['Valid Rows'].sum()
        invalid_remove_total = quality_df['Invalid Removed Rows'].sum()
        invalid_fix_total = quality_df['Invalid Fixed Rows'].sum()
        # Pie chart data
        sizes = [valid_total, invalid_remove_total, invalid_fix_total]
        labels = ['Valid', 'Invalid Remove', 'Invalid Fix']
        colors = ['#2ECC71', '#E74C3C', '#F1C40F']

        plt.figure(figsize=(7,7))
        plt.pie(
            sizes,
            labels=labels,
            autopct=lambda pct: f"{int(round(pct/100*sum(sizes)))} rows\n({pct:.1f}%)",
            colors=colors,
            startangle=90,
            wedgeprops={'edgecolor':'black'},
            textprops={'fontsize':14, 'weight':'bold'}
        )
        plt.title('Overall Data Quality', fontsize=20, weight='bold')
        plt.legend(labels, loc='lower center', bbox_to_anchor=(0.5, -0.05), fontsize=12, ncol=3)
        plt.tight_layout()
        plt.savefig(os.path.join(etl_folder, f"Data_Quality_Pie_{timestamp}.png"))
        plt.close()


        # Bar chart
        if summary_records:
            summary_df = pd.DataFrame(summary_records)
            summary_counts = summary_df.groupby('Inconsistency Type')['Distinct Count of Rows'].sum().sort_values(ascending=False)
            plt.figure(figsize=(12,7))
            bars = plt.bar(summary_counts.index, summary_counts.values, color=plt.cm.tab20.colors)
            plt.ylabel('Number of Rows', fontsize=14, weight='bold')
            plt.title('Inconsistency Types Summary', fontsize=20, weight='bold')
            plt.xticks(rotation=45, ha='right', fontsize=12)
            plt.yticks(fontsize=12)
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width()/2, height + 1, f'{int(height)}', ha='center', va='bottom', fontsize=12, weight='bold')
            plt.tight_layout()
            plt.savefig(os.path.join(etl_folder, f"Inconsistency_Types_Bar_{timestamp}.png"))
            plt.close()


    print("Scan Complete", f"✅ Inconsistencies scanned and saved at:\n{report_file_with_time}")
    return df



def clean_dataset(save_path, inconsistent_df):
    print("Cleaning dataset...")

    df = inconsistent_df.copy()
    dropped_rows = []  # to collect dropped rows + reasons

    # ---------- 1️⃣ Remove negative sales ----------
    if 'sales' in df.columns:
        negative_mask = df['sales'] < 0
        if negative_mask.any():
            tmp = df[negative_mask].copy()
            tmp['Drop_Reason'] = "Negative sales"
            dropped_rows.append(tmp)
        df = df[~negative_mask]

    # ---------- 2️⃣ Remove Quantity-Profit inconsistencies ----------
    if {'quantity', 'profit'}.issubset(df.columns):
        qp_mask = (df['quantity'] < 0) & (df['profit'] > 0)
        if qp_mask.any():
            tmp = df[qp_mask].copy()
            tmp['Drop_Reason'] = "Quantity-Profit Logic"
            dropped_rows.append(tmp)
        df = df[~qp_mask]

    # ---------- 3️⃣ Remove Quantity-Sales inconsistencies ----------
    if {'quantity', 'sales'}.issubset(df.columns):
        qs_mask = (df['quantity'] == 0) & (df['sales'] > 0)
        if qs_mask.any():
            tmp = df[qs_mask].copy()
            tmp['Drop_Reason'] = "Quantity-Sales Logic"
            dropped_rows.append(tmp)
        df = df[~qs_mask]

    # ---------- 4️⃣ Handle duplicates ----------
    if {'order_id', 'product_id', 'sales'}.issubset(df.columns):
        df = df.sort_values(by='sales', ascending=False)
        dup_mask = df.duplicated(subset=['order_id', 'product_id'], keep='first')
        if dup_mask.any():
            tmp = df[dup_mask].copy()
            tmp['Drop_Reason'] = "Duplicate order_id/product_id (kept highest sales)"
            dropped_rows.append(tmp)
        df = df[~dup_mask]

   
    date_columns = ['order_date_Date_Format_Solution', 'ship_date_Date_Format_Solution']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col],  # the column values
                dayfirst=True,           # interpret as DD-MM-YYYY
                errors='coerce'          # invalid parsing becomes NaT
            )
    # ---------- 5️⃣ Fix dates directly ----------
    if 'order_date_Date_Format_Solution' in df.columns:
        df['order_date'] = df['order_date_Date_Format_Solution']
    if 'ship_date_Date_Format_Solution' in df.columns:
        df['ship_date'] = df['ship_date_Date_Format_Solution']

    # ---------- 6️⃣ Fix postal code directly ----------
    if 'Postal_Code_Solution' in df.columns:
        df['postal_code'] = df['Postal_Code_Solution']

    # ---------- 7️⃣ Reorder columns ----------
    desired_order = [
        'row_id', 'order_id', 'order_date', 'ship_date', 'ship_mode', 'customer_id', 'customer_name',
        'segment', 'country', 'city', 'state', 'postal_code', 'region',
        'product_id', 'category', 'sub_category', 'product_name', 'sales', 'quantity', 'discount', 'profit'
    ]
    desired_order = [col for col in desired_order if col in df.columns]
    cleaned_df = df[desired_order]

    # ---------- 8️⃣ Save cleaned dataset as Excel ----------
    cleaned_file = os.path.join(
        save_path, f"Cleaned_Dataset_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    )
    cleaned_df.to_excel(cleaned_file, index=False, engine='openpyxl')
    print(f"✅ Cleaned dataset saved to: {cleaned_file}")

    # ---------- 🔟 Save dropped rows (if any) ----------
    if dropped_rows:
        dropped_df = pd.concat(dropped_rows, ignore_index=True)
        dropped_file = os.path.join(
            save_path, f"Dropped_Rows_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        )
        dropped_df.to_excel(dropped_file, index=False, engine='openpyxl')
        print(f"⚠️ Dropped rows saved to: {dropped_file}")
    else:
        print("✅ No rows were dropped")

    # ---------- 9️⃣ Check missing row IDs ----------
    id_col = "row_id"
    if id_col in df.columns:
        min_id, max_id = df[id_col].min(), df[id_col].max()
        print(f"ℹ️ Smallest {id_col}: {min_id}")
        print(f"ℹ️ Largest  {id_col}: {max_id}")

        full_range = set(range(min_id, max_id + 1))
        present = set(df[id_col].dropna().astype(int))
        missing = sorted(full_range - present)

        if missing:
            print(f"❌ Missing {id_col}s: {missing}")
        else:
            print("✅ No missing IDs found")
    else:
        print(f"⚠️ Column '{id_col}' not found in DataFrame")

    return cleaned_df


# ---------- POSTGRESQL ETL FUNCTIONS ----------
def create_schemas_if_not_exist():
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS stg;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mdm;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dwh;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dm;"))
        conn.commit()
    print("✅ Schemas ensured")

    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS stg.stg_orders_raw (
            row_id integer NOT NULL, 
            order_id character varying(64), 
            order_date date, 
            ship_date date, 
            ship_mode character varying(64), 
            customer_id character varying(64), 
            customer_name character varying(256), 
            segment character varying(64), 
            country character varying(128), 
            city character varying(128), 
            state character varying(128), 
            postal_code character varying(32), 
            region character varying(64), 
            product_id character varying(64), 
            category character varying(64), 
            sub_category character varying(64), 
            product_name character varying(256), 
            sales numeric(18,2), 
            quantity integer, 
            discount numeric(9,4), 
            profit numeric(18,2), 
            source_file_name character varying(256) NOT NULL, 
            source_month character(6), 
            batch_loaded_at timestamp without time zone, 
            batch_seq bigint
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS mdm.mdm_dim_customer (
        customer_sk BIGSERIAL NOT NULL,  -- surrogate key, optional
        customer_id VARCHAR(64) NOT NULL,
        customer_name VARCHAR(256),
        segment VARCHAR(64),
        is_current BOOLEAN,
        effective_from TIMESTAMP WITHOUT TIME ZONE,
        effective_to TIMESTAMP WITHOUT TIME ZONE,
        CONSTRAINT customer_pk PRIMARY KEY (customer_id)  -- business key
        );

        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.dwh_fact_sales_order (
        order_id character varying(64) NOT NULL, 
        order_date date NOT NULL, 
        ship_date date, 
        delivery_days integer, 
        ship_mode character varying(64), 
        customer_sk bigint, 
        product_sk bigint, 
        geography_sk bigint, 
        sales numeric(18,2), 
        quantity integer, 
        discount numeric(9,4), 
        profit numeric(18,2), 
        source_month character(6), 
        batch_loaded_at timestamp without time zone,
        PRIMARY KEY (order_id, product_sk)
    );
        """,
        """
        CREATE TABLE IF NOT EXISTS mdm.mdm_dim_product (
        product_sk BIGSERIAL NOT NULL,  -- surrogate key
        product_id VARCHAR(64) NOT NULL PRIMARY KEY,  -- natural/business key
        product_name VARCHAR(256),
        sub_category VARCHAR(64),
        category VARCHAR(64),
        is_current BOOLEAN,
        effective_from TIMESTAMP WITHOUT TIME ZONE,
        effective_to TIMESTAMP WITHOUT TIME ZONE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS mdm.mdm_dim_geography (
        geography_sk BIGSERIAL NOT NULL,  -- surrogate key
        country VARCHAR(128),
        state VARCHAR(128),
        city VARCHAR(128) NOT NULL,
        postal_code VARCHAR(32) NOT NULL,
        region VARCHAR(64),
        CONSTRAINT geography_pk PRIMARY KEY (city, postal_code)  -- natural/business key
        );
        """
    ]

    with engine.connect() as conn:
        for query in create_table_queries:
            conn.execute(text(query))
        conn.commit()  # commit the transaction

    print("✅ Schemas ensured")


def load_to_staging(df):
    """
    Load a cleaned DataFrame to PostgreSQL staging table stg.stg_orders_raw
    with duplicate handling, normalization, numeric conversion, and metadata.
    """

    if df is None or df.empty:
        print("⚠️ No data to load")
        return

    # Print number of rows in DataFrame
    print(f"ℹ️ Total rows to process: {len(df)}")

    # ---------- 1️⃣ Get existing data ----------
    with engine.connect() as conn:
        existing = pd.read_sql("SELECT order_id, product_id, sales FROM stg.stg_orders_raw", conn)
        existing_count = len(existing)

    print(f"ℹ️ Already in DB: {existing_count} rows")

    # ---------- 2️⃣ Remove duplicates in the new DataFrame ----------
    if {'order_id', 'product_id', 'sales'}.issubset(df.columns):
        # Sort so rows with higher sales come first
        df = df.sort_values(by='sales', ascending=False)
        # Drop duplicates inside incoming batch
        df = df.drop_duplicates(subset=['order_id', 'product_id'], keep='first')

    after_internal_dedup = len(df)

    # ---------- 3️⃣ Identify new rows vs. duplicates ----------
    # Merge with existing DB to find duplicates
    merged = df.merge(existing[['order_id', 'product_id']], 
                      on=['order_id', 'product_id'], 
                      how='left', indicator=True)

    # Keep only new rows
    new_rows = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
    new_rows_count = len(new_rows)
    duplicate_count = len(df) - new_rows_count

    print(f"ℹ️ After internal dedup: {after_internal_dedup} rows")
    print(f"❌ Duplicates skipped (already in DB): {duplicate_count}")
    print(f"✅ New rows to insert: {new_rows_count}")

    if new_rows_count == 0:
        print("⚠️ No new rows to insert")
        return

    # ---------- 4️⃣ Add metadata ----------
    new_rows['source_file_name'] = 'clean_dataset'

    # ---------- 5️⃣ Insert into staging ----------
    new_rows.to_sql('stg_orders_raw', engine, schema='stg', if_exists='append', index=False)

    # ---------- 6️⃣ Final count in DB ----------
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM stg.stg_orders_raw"))
        final_count = result.scalar()

    print(f"✅ Successfully inserted {new_rows_count} new rows")
    print(f"ℹ️ Total rows now in stg.stg_orders_raw: {final_count}")



def populate_master_tables(engine):
    """
    Populate master dimension tables (Customer, Product, Geography)
    from staging table stg.stg_orders_raw.
    Inserts only distinct records and avoids duplicates with existing data.
    """
    import pandas as pd

    # ---------- 1️⃣ Read staging data ----------
    with engine.connect() as conn:
        df = pd.read_sql("SELECT * FROM stg.stg_orders_raw", conn)

    # ---------- 2️⃣ Customers ----------
    df_customers = df[['customer_id', 'customer_name', 'segment']].drop_duplicates(subset=['customer_id'])
    df_customers['is_current'] = True
    df_customers['effective_from'] = pd.Timestamp.now()
    df_customers['effective_to'] = pd.NaT

    # Get existing customer IDs
    existing_customers = pd.read_sql("SELECT customer_id FROM mdm.mdm_dim_customer", engine)
    df_customers = df_customers[~df_customers['customer_id'].isin(existing_customers['customer_id'])]

    if not df_customers.empty:
        df_customers.to_sql(
            'mdm_dim_customer',
            engine,
            schema='mdm',
            if_exists='append',
            index=False,
            method='multi'
        )

    # ---------- 3️⃣ Products ----------
    df_products = df[['product_id', 'product_name', 'sub_category', 'category']].drop_duplicates(subset=['product_id'])
    df_products['is_current'] = True
    df_products['effective_from'] = pd.Timestamp.now()
    df_products['effective_to'] = pd.NaT

    # Get existing product IDs
    existing_products = pd.read_sql("SELECT product_id FROM mdm.mdm_dim_product", engine)
    df_products = df_products[~df_products['product_id'].isin(existing_products['product_id'])]

    if not df_products.empty:
        df_products.to_sql(
            'mdm_dim_product',
            engine,
            schema='mdm',
            if_exists='append',
            index=False,
            method='multi'
        )

    # ---------- 4️⃣ Geography ----------
    df_geo = df[['country', 'state', 'city', 'postal_code', 'region']].drop_duplicates()

    # Get existing geo keys
    existing_geo = pd.read_sql(
        "SELECT country, state, city, postal_code, region FROM mdm.mdm_dim_geography", engine
    )
    # Merge to find new rows only
    df_geo = pd.merge(df_geo, existing_geo, how='outer', indicator=True)
    df_geo = df_geo[df_geo['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not df_geo.empty:
        df_geo.to_sql(
            'mdm_dim_geography',
            engine,
            schema='mdm',
            if_exists='append',
            index=False,
            method='multi'
        )

    print("✅ Master tables populated successfully (distinct snapshot, old data preserved)")


def populate_fact_table(engine):
    import pandas as pd

    # 1️⃣ Read staging data with required filters
    df_orders = pd.read_sql("""
        SELECT *
        FROM stg.stg_orders_raw
        WHERE customer_id IS NOT NULL
          AND product_id IS NOT NULL
          AND order_date IS NOT NULL
    """, engine)

    if df_orders.empty:
        print("⚠️ No orders to insert into fact table")
        return

    # 2️⃣ Calculate delivery_days
    df_orders['delivery_days'] = (
        pd.to_datetime(df_orders['ship_date']) - pd.to_datetime(df_orders['order_date'])
    ).dt.days

    # 3️⃣ Map surrogate keys
    with engine.connect() as conn:
        df_customers = pd.read_sql("SELECT customer_sk, customer_id FROM mdm.mdm_dim_customer", conn)
        df_products = pd.read_sql("SELECT product_sk, product_id FROM mdm.mdm_dim_product", conn)
        df_geo = pd.read_sql("SELECT geography_sk, country, state, city, postal_code, region FROM mdm.mdm_dim_geography", conn)

    df_orders = df_orders.merge(df_customers, on='customer_id', how='left')
    df_orders = df_orders.merge(df_products, on='product_id', how='left')
    df_orders = df_orders.merge(
        df_geo,
        on=['country', 'state', 'city', 'postal_code', 'region'],
        how='left'
    )

    # 4️⃣ Remove rows already in fact table
    existing_facts = pd.read_sql("SELECT order_id, product_sk FROM dwh.dwh_fact_sales_order", engine)
    df_orders = df_orders.merge(existing_facts, on=['order_id', 'product_sk'], how='left', indicator=True)
    df_new = df_orders[df_orders['_merge'] == 'left_only'].drop(columns=['_merge'])

    if df_new.empty:
        print("⚠️ No new rows to insert into fact table")
        return

    # 5️⃣ Select columns
    fact_cols = [
        'order_id', 'order_date', 'ship_date', 'delivery_days', 'ship_mode',
        'customer_sk', 'product_sk', 'geography_sk', 'sales', 'quantity', 'discount', 'profit',
        'source_month', 'batch_loaded_at'
    ]
    df_fact = df_new[fact_cols]

    # 6️⃣ Append
    df_fact.to_sql(
        'dwh_fact_sales_order',
        engine,
        schema='dwh',
        if_exists='append',
        index=False,
        method='multi'
    )

    print(f"✅ Fact table populated with {len(df_fact)} new rows")



def export_data_marts(engine, base_save_path):
    # 1️⃣ Ensure dm schema exists
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dm;"))

    # ---------- 2️⃣ Sales by State & Month ----------
    df_state_month = pd.read_sql("""
        SELECT 
        g.state,
        TO_CHAR(f.order_date, 'YYYY-MM') AS month,
        SUM(f.sales) AS total_sales,
        SUM(f.quantity) AS total_quantity,
        SUM(f.sales)::numeric / NULLIF(SUM(f.quantity),0) AS avg_price,
        AVG(f.ship_date - f.order_date) AS avg_delivery_days,
        SUM(f.profit) AS total_profit,
        SUM(f.profit)::numeric / NULLIF(SUM(f.sales),0) AS profit_margin,
        COUNT(DISTINCT f.customer_sk) AS distinct_customers
    FROM dwh.dwh_fact_sales_order f
    JOIN mdm.mdm_dim_geography g ON f.geography_sk = g.geography_sk
    GROUP BY g.state, TO_CHAR(f.order_date, 'YYYY-MM')
    """, engine)

    df_state_month.to_sql(
        'dm_sales_by_state_month', engine, schema='dm', if_exists='replace', index=False
    )

    # ---------- 3️⃣ Best Customers per Segment ----------
    df_best_customers = pd.read_sql("""
        SELECT *
        FROM (
            SELECT 
                c.customer_id,
                c.customer_name,
                c.segment,
                SUM(f.sales) AS total_sales,
                DENSE_RANK() OVER(PARTITION BY c.segment ORDER BY SUM(f.sales) DESC) AS rank_in_segment
            FROM dwh.dwh_fact_sales_order f
            JOIN mdm.mdm_dim_customer c ON f.customer_sk = c.customer_sk
            GROUP BY c.customer_id, c.customer_name, c.segment
        ) AS ranked
        WHERE rank_in_segment <= 10
    """, engine)

    df_best_customers.to_sql(
        'dm_best_customers', engine, schema='dm', if_exists='replace', index=False
    )

    # ---------- 4️⃣ Top/Bottom Products by Sales per State ----------
    df_products_state = pd.read_sql("""
        SELECT *
        FROM (
            SELECT 
                p.product_id,
                p.product_name,
                g.state,
                SUM(f.sales) AS total_sales,
                DENSE_RANK() OVER(PARTITION BY g.state ORDER BY SUM(f.sales) DESC) AS rank_top,
                DENSE_RANK() OVER(PARTITION BY g.state ORDER BY SUM(f.sales) ASC) AS rank_bottom
            FROM dwh.dwh_fact_sales_order f
            JOIN mdm.mdm_dim_product p ON f.product_sk = p.product_sk
            JOIN mdm.mdm_dim_geography g ON f.geography_sk = g.geography_sk
            GROUP BY p.product_id, p.product_name, g.state
        ) AS ranked
    """, engine)

    df_products_state.to_sql(
        'dm_products_by_state', engine, schema='dm', if_exists='replace', index=False
    )

    print("✅ Data marts created in dm schema successfully")

   
    # ---------- 5️⃣ Save all marts to one Excel file in Datamarts folder ----------
    datamart_folder = os.path.join(base_save_path, "Datamarts")
    os.makedirs(datamart_folder, exist_ok=True)
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    excel_file = os.path.join(datamart_folder, f"DataMarts_{timestamp}.xlsx")

    # Save all data marts to Excel
    with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
        df_state_month.to_excel(writer, sheet_name='Sales_by_State_Month', index=False)
        df_best_customers.to_excel(writer, sheet_name='Best_Customers', index=False)
        df_products_state.to_excel(writer, sheet_name='Products_by_State', index=False)

    print(f"✅ Data marts saved to Excel: {excel_file}")

    csv_folder = os.path.join(datamart_folder, "CSV_Files")
    os.makedirs(csv_folder, exist_ok=True)

    data_marts = {
        'dm_sales_by_state_month': df_state_month,
        'dm_best_customers': df_best_customers,
        'dm_products_by_state': df_products_state
    }

    summary_records = []

    for name, df in data_marts.items():
        csv_path = os.path.join(csv_folder, f"{name}.csv")
        df.to_csv(csv_path, index=False)

        row_count = df.shape[0]

        # Determine primary key column automatically
        if 'row_id' in df.columns:
            pk_col = 'row_id'
        elif 'customer_id' in df.columns:
            pk_col = 'customer_id'
        elif 'product_id' in df.columns:
            pk_col = 'product_id'
        elif 'state' in df.columns:
            pk_col = 'state'
        else:
            pk_col = None

        pk_count = df[pk_col].nunique() if pk_col else row_count

        summary_records.append({
            'Data Mart System Name': name,
            'Count Rows': row_count,
            'Count Distinct Primary Key': pk_count
        })

    # Save summary CSV
    summary_csv = os.path.join(datamart_folder, "Task_6_2_Data_Marts_Rows.csv")
    pd.DataFrame(summary_records).to_csv(summary_csv, index=False)
    print(f"✅ Summary CSV saved: {summary_csv}")

    # Zip all CSVs
    zip_file = os.path.join(datamart_folder, "Task_6_1_Data_Marts.zip")
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zf:
        for csv_file in os.listdir(csv_folder):
            zf.write(os.path.join(csv_folder, csv_file), arcname=csv_file)

    print(f"✅ All CSVs zipped: {zip_file}")


# --------------- Main FUNCTION ---------------
def main():
    try:
        save_path = save_path_var.get()
        if not save_path:
            messagebox.showwarning("Warning", "Please select a folder first.")
            return

        # Add timestamp subfolder
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        save_path = os.path.join(save_path, timestamp)
        os.makedirs(save_path, exist_ok=True)

        # ---------- 1. Download files ----------
        downloaded_files = download_files_from_sftp(save_path)
        if not downloaded_files:
            return

        # ---------- 2. Extract ----------
        extract_archives(save_path)

        # ---------- 3. Merge ----------
        merged_df = merge_all_csv_files(save_path)
        print(merged_df.head(5))

        # ---------- 4. Scan inconsistencies ----------
        inconsistent_df = scan_inconsistencies(save_path,merged_df)
        print(inconsistent_df.head(5))

        # ---------- 5. Clean Dataset -----------
        clean_dataset_df = clean_dataset(save_path,inconsistent_df)

        # ---------- 6. PostgreSQL ETL ----------
        create_schemas_if_not_exist()

        # ---------- 7. load_to_staging ----------
        load_to_staging(clean_dataset_df)

        # ---------- 8. populate_master_tables ----------
        populate_master_tables(engine)

        # ---------- 9. populate_fact_table ----------
        populate_fact_table(engine)

        # ---------- 10. export_data_marts ----------    
        export_data_marts(engine, save_path)  



        source_path = r"D:\DataScience\Python\Python Code\d\Case_Study.py"
        destination_folder = r"D:\DataScience\Python\Python Code\d\Retail_CaseStudy_Deliverables\2. Correct Data\1. code"
        os.makedirs(destination_folder, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        destination_path = os.path.join(destination_folder, f"Case_Study_{timestamp}.py")
        shutil.copy2(source_path, destination_path)
        print(f"✅ File copied with timestamp to: {destination_path}")


        # Close the app after successful run
        messagebox.showinfo("ETL Complete", "✅ ETL process completed successfully!")
        root.destroy()  # <-- This will close the Tkinter window

    except Exception as e:
        # --- Log error to file ---
        log_file = os.path.join(save_path if 'save_path' in locals() else ".", "error_log.txt")
        with open(log_file, "a", encoding="utf-8") as f:
            f.write("\n" + "="*60 + "\n")
            f.write(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(traceback.format_exc())
            f.write("="*60 + "\n")

        # Show error popup
        messagebox.showerror("Error", f"Unexpected error:\n{e}")
        print("Error logged to:", log_file)

# ---------- TKINTER GUI ----------
root = tk.Tk()
root.title("SFTP ETL & Data Quality Pipeline")
root.geometry("600x250")

save_path_var = tk.StringVar()

tk.Label(root, text="Select folder to download files and run ETL:").pack(pady=5)
tk.Entry(root, textvariable=save_path_var, width=70).pack(pady=5)
tk.Button(root, text="Browse", command=select_save_path).pack(pady=5)
tk.Button(root, text="Run Full ETL Pipeline", command=main).pack(pady=10)

root.mainloop()
