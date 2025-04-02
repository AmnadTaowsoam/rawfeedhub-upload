import pandas as pd
import os
import json
from core.logging import get_logger

logger = get_logger(__name__)

def load_json(file_path):
    """
    ฟังก์ชันสำหรับโหลดไฟล์ JSON
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Error loading JSON file {file_path}: {e}")
        return {}

def rename_columns(df, column_mapping):
    """
    ฟังก์ชันสำหรับเปลี่ยนชื่อคอลัมน์ใน DataFrame ตาม mapping
    โดยทำการ Trim ชื่อคอลัมน์เพื่อจัดการช่องว่างและ Enter ก่อน
    """
    try:
        df.columns = df.columns.str.strip().str.replace('\n', '', regex=True)
        df = df.rename(columns=column_mapping)
        unwanted_columns = ['CONCATENATE', 'Operation short text']
        df = df.drop(columns=[col for col in unwanted_columns if col in df.columns], errors='ignore')
        logger.info("Columns renamed and unwanted columns dropped successfully.")
        return df
    except Exception as e:
        logger.error(f"Error renaming columns: {e}")
        raise
    
def clean_text_column(df, text_column):
    """Clean a specific column by removing single and double quotes."""
    if text_column in df.columns:
        df[text_column] = df[text_column].astype(str)
        df[text_column] = df[text_column].str.replace("'", "", regex=False)
        df[text_column] = df[text_column].str.replace('"', "", regex=False)
    return df

def add_missing_columns(df, required_columns):
    """
    ฟังก์ชันสำหรับเพิ่ม Column ที่ขาดหายให้ครบตาม required_columns
    """
    try:
        for column in required_columns:
            if column not in df.columns:
                df[column] = None  # เติม Column ใหม่ที่มีค่าเริ่มต้นเป็น None
        logger.info("Missing columns added successfully.")
        return df
    except Exception as e:
        logger.error(f"Error adding missing columns: {e}")
        raise

def clean_date_column(df, date_column):
    """
    ฟังก์ชันสำหรับ Clean ข้อมูลใน Column วันที่ให้อยู่ในรูปแบบ YYYY-MM-DD
    """
    try:
        if date_column in df.columns:
            df[date_column] = pd.to_datetime(df[date_column], dayfirst=True, errors='coerce')
            logger.info(f"Date column '{date_column}' cleaned successfully.")
        return df
    except Exception as e:
        logger.error(f"Error cleaning date column '{date_column}': {e}")
        raise

def enforce_data_types(df, schema):
    """
    ฟังก์ชันสำหรับบังคับประเภทข้อมูลตาม Schema
    """
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

def export_to_parquet(df, output_folder, filename):
    """
    ฟังก์ชันสำหรับ Export DataFrame เป็น Parquet ไฟล์
    """
    try:
        os.makedirs(output_folder, exist_ok=True)  # สร้างโฟลเดอร์หากยังไม่มี
        output_filepath = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}.parquet")
        df.to_parquet(output_filepath, index=False)
        logger.info(f"Exported cleaned file to: {output_filepath}")
    except Exception as e:
        logger.error(f"Error exporting file '{filename}' to Parquet: {e}")
        raise

def clean_and_process_files(input_folder, output_folder, mapping_file_path, schema_file_path, date_column):
    """
    ฟังก์ชันหลักสำหรับอ่านไฟล์ Excel, Clean ข้อมูล, และ Export Parquet
    """
    try:
        column_mapping = load_json(mapping_file_path)
        schema = load_json(schema_file_path)

        if not column_mapping or not schema:
            logger.error("Missing column mapping or schema. Please check your JSON files.")
            return

        required_columns = list(column_mapping.values())  # คอลัมน์ทั้งหมดที่ต้องมี

        for filename in os.listdir(input_folder):
            if filename.endswith('.parquet'):
                filepath = os.path.join(input_folder, filename)
                logger.info(f"Processing file: {filename}")
                try:
                    # โหลดไฟล์ parquet
                    df = pd.read_parquet(filepath)
                    df = rename_columns(df, column_mapping)
                    df = add_missing_columns(df, required_columns)
                    df = clean_date_column(df, date_column)
                    df = enforce_data_types(df, schema)
                    
                    # Export ไฟล์ Parquet
                    export_to_parquet(df, output_folder, filename)
                except Exception as e:
                    logger.error(f"Error processing file '{filename}': {e}")
    except Exception as e:
        logger.error(f"Error in clean_and_process_files: {e}")
        raise
