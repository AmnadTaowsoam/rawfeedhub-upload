import pandas as pd
import os
import json
import logging

# Configure Logging
logging.basicConfig(
    filename="clean_data.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def load_json(file_path):
    """Load a JSON file and return its content."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        return {}

def clean_and_rename_columns(df, column_mapping):
    """Clean and rename columns based on the provided mapping."""
    try:
        df.columns = df.columns.str.lower().str.strip().str.replace('\n', '', regex=True)
        df = df.rename(columns=column_mapping)
        unwanted_columns = ['concatenate', 'operation short text', 'usage decision code']
        df = df.drop(columns=[col for col in unwanted_columns if col in df.columns], errors='ignore')
        logger.info("Columns cleaned and renamed successfully.")
        return df
    except Exception as e:
        logger.error(f"Error renaming columns: {e}")
        raise

def clean_text_columns(df, columns):
    """
    Clean specified text columns by:
    - Removing single and double quotes
    - Replacing empty strings, "None", and spaces with NaN
    """
    for col in columns:
        if col in df.columns:
            # แปลงค่าเป็น string
            df[col] = df[col].astype(str)
            # ลบ single quotes และ double quotes
            df[col] = df[col].str.replace("'", "", regex=False).str.replace('"', "", regex=False)
            # แทนค่าที่เป็นช่องว่าง "", "None", หรือ " " ด้วย NaN
            df[col] = df[col].replace({"": pd.NA, "None": pd.NA, " ": pd.NA})
    return df

def add_missing_columns(df, required_columns):
    """Add any missing columns with None as default values."""
    try:
        for column in required_columns:
            if column not in df.columns:
                df[column] = None
        logger.info("Missing columns added successfully.")
        return df
    except Exception as e:
        logger.error(f"Error adding missing columns: {e}")
        raise

def clean_date_column(df, date_column):
    """Clean and format date column to YYYY-MM-DD."""
    try:
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
            logger.info(f"Date column '{date_column}' cleaned successfully.")
        return df
    except Exception as e:
        logger.error(f"Error cleaning date column '{date_column}': {e}")
        raise

def enforce_data_types(df, schema):
    """Enforce data types on the DataFrame based on the schema."""
    try:
        for column, dtype in schema.items():
            if column in df.columns:
                if dtype == 'datetime64':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                else:
                    df[column] = df[column].astype(dtype, errors='ignore')
        logger.info("Data types enforced successfully.")
        return df
    except Exception as e:
        logger.error(f"Error enforcing data types: {e}")
        raise

def convert_thai_date_to_gregorian(df, date_column):
    """Convert Thai Buddhist calendar dates to Gregorian calendar."""
    try:
        if date_column in df.columns:
            df[date_column] = df[date_column].str.strip()
            df[date_column] = df[date_column].str.replace(r'[^0-9/]', '', regex=True)
            df[date_column] = df[date_column].apply(
                lambda x: '/'.join([x.split('/')[0], x.split('/')[1], str(int(x.split('/')[2]) - 543)])
                if len(x.split('/')) == 3 else x
            )
            df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y', errors='coerce')
            df = df.dropna(subset=[date_column])
            logger.info(f"Date column '{date_column}' converted and cleaned successfully.")
        return df
    except Exception as e:
        logger.error(f"Error converting Thai date column '{date_column}': {e}")
        raise
    
def clear_nan_column(df):
    columns_to_check = [
        'moisture', 'ash', 'protein', 'fat', 'fiber', 'p', 'ca', 'insoluble', 'nacl', 'ffa', 'ua',
        'kohps', 'brix', 'pepsin', 'pepsin0002', 'ndf', 'adf', 'adl', 'eth', 't_fat', 'tvn', 'nh3',
        'starch', 'iv', 'pv', 'av', 'totox', 'p_anisidine', 'xanthophyll', 'ac_insol', 'gluten',
        'sulfer', 'sulfate'
    ]
    # ตรวจสอบจำนวนแถวที่ทุกค่าใน columns_to_check เป็น NaN/None
    nan_rows = df[columns_to_check].isnull().all(axis=1)
    print(f"จำนวนแถวที่ทุกค่าใน columns_to_check เป็น NaN/None: {nan_rows.sum()}")
    
    # ลบแถวที่ทุกค่าใน columns_to_check เป็น NaN/None
    df = df.dropna(subset=columns_to_check, how='all')
    
    # ตรวจสอบจำนวนแถวหลังจากลบแถวที่ทุกค่าเป็น NaN
    print(f"จำนวนแถวหลังการลบ: {len(df)}")
    return df

def export_to_parquet(df, output_folder, filename):
    """Export DataFrame to a Parquet file."""
    try:
        os.makedirs(output_folder, exist_ok=True)
        output_filepath = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.parquet")
        df.to_parquet(output_filepath, index=False)
        logger.info(f"Exported cleaned file to: {output_filepath}")
    except Exception as e:
        logger.error(f"Error exporting file '{filename}' to Parquet: {e}")
        raise

# File paths and configurations
filepath = 'D:/Betagro Public Company Limited/Agro Report Analysis - RM_Process Data/Db_RMA_2017.xlsx'
output_folder = 'D:/Betagro Public Company Limited/Agro Report Analysis - RM_Process Data/Quality_Database-RMAnalysis'
mapping_file = 'column_mapping.json'
schema_file = 'schema_data_type.json'
date_column = 'date'

# Load configurations
column_mapping = load_json(mapping_file)
schema = load_json(schema_file)

# Load and process data
try:
    df_raw = pd.read_excel(filepath, sheet_name='Db_Export')
    df = clean_and_rename_columns(df_raw, column_mapping)
    df = add_missing_columns(df, column_mapping.values())
    df = clean_text_columns(df, column_mapping.values())
    df = convert_thai_date_to_gregorian(df, date_column)
    df = clean_date_column(df, date_column)
    df = enforce_data_types(df, schema)
    df = clear_nan_column(df)

    # Export the cleaned DataFrame
    filename = os.path.basename(filepath)
    export_to_parquet(df, output_folder, filename)

except Exception as e:
    logger.error(f"Error processing file: {e}")
