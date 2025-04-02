## services/data_cleansing.py

import os
import pandas as pd
import shutil
from app.master_data.master_column import master_column, active_columns, new_column, numeric_cols, string_cols, date_cols

class CleanFile:
    def __init__(self):
        self.raw_folder = './data/raw/'
        self.output_folder = './data/clean_file/'
        self.complete_folder = './data/complete_file/'
        # สร้างโฟลเดอร์ output และ complete ถ้ายังไม่มี
        os.makedirs(self.output_folder, exist_ok=True)
        os.makedirs(self.complete_folder, exist_ok=True)
    
    def load_data(self, path_file: str, sheet_name: str = None):
        df = pd.read_excel(path_file, sheet_name=sheet_name)
        # ตรวจสอบและเพิ่มคอลัมน์ที่หายไป
        missing_cols = set(active_columns) - set(df.columns)
        for col in missing_cols:
            df[col] = None  # กำหนดค่าเริ่มต้นเป็น None
        df = df[active_columns]
        return df
    
    def rename_columns(self, df):
        # สร้าง dictionary mapping จาก active_columns เป็น new_column
        mapping = dict(zip(active_columns, new_column))
        df = df.rename(columns=mapping)
        return df
    
    def clean_dataframe(self, df):
        # สำหรับคอลัมน์ที่ควรเป็นตัวเลข ให้แปลงค่าโดยใช้ pd.to_numeric และกำหนด errors='coerce'
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        
        # สำหรับคอลัมน์ข้อความ ให้แทนที่ค่า NaN ด้วยค่าว่าง และแปลงค่าให้เป็นสตริง
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str)
        
        # แปลงคอลัมน์วันที่ให้เป็นรูปแบบ Date
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            
        # รับรองว่าคอลัมน์ pallet_no เป็น string โดยเฉพาะ
        if 'pallet_no' in df.columns:
            df['pallet_no'] = df['pallet_no'].astype(str)
        
        if 'load_time' in df.columns and 'manufacturing_date' in df.columns:
            # แปลง manufacturing_date เป็น string และ load_time เป็น string แล้วต่อกัน
            df['load_time'] = pd.to_datetime(df['manufacturing_date'].astype(str) + ' ' + df['load_time'].astype(str),errors='coerce')
        return df
    
    def pipeline(self, path_file: str, sheet_name: str = None):
        try:
            df = self.load_data(path_file, sheet_name=sheet_name)
            df = self.rename_columns(df)
            df = self.clean_dataframe(df)
            return df
        except Exception as e:
            print(f"Error processing {path_file}: {e}")
            return None
    
    def process_all_files(self, sheet_name='Db_FP', combine_by_year=False):
        """
        Process all Excel files in the raw folder structure.
        - ถ้า combine_by_year = False: แต่ละไฟล์จะถูกแปลงและเซฟเป็นไฟล์ Parquet แยกไฟล์
        - ถ้า combine_by_year = True: จะรวมข้อมูลของไฟล์ในแต่ละปีเข้าด้วยกันแล้วเซฟเป็นไฟล์เดียวต่อปี
        หลังจากประมวลผลสำเร็จ ไฟล์ต้นฉบับจะถูกย้ายไปยัง complete folder
        """
        # ถ้า combine_by_year เป็น True ให้เก็บ DataFrame ของแต่ละปีใน dictionary
        combined_data = {}
        
        # เดินทางผ่านทุกไฟล์ใน self.raw_folder
        for root, dirs, files in os.walk(self.raw_folder):
            for file in files:
                if file.lower().endswith(('.xlsx', '.xls')):
                    file_path = os.path.join(root, file)
                    print(f"Processing file: {file_path}")
                    df = self.pipeline(file_path, sheet_name=sheet_name)
                    if df is not None:
                        # กำหนด folder และ subfolder จากเส้นทางสัมพัทธ์ (relative path)
                        rel_path = os.path.relpath(root, self.raw_folder)
                        parts = rel_path.split(os.sep)
                        # สมมติว่า subfolder ที่เป็นปีจะเป็นตัวเลข 4 หลัก
                        year = None
                        for part in parts:
                            if part.isdigit() and len(part) == 4:
                                year = part
                                break
                        if not year:
                            year = "unknown"
                        
                        if combine_by_year:
                            if year not in combined_data:
                                combined_data[year] = []
                            combined_data[year].append(df)
                        else:
                            # ตั้งชื่อไฟล์ใหม่เป็น {folder}_{year}_{original_filename}.parquet
                            folder_name = parts[0] if parts else "root"
                            file_base = os.path.splitext(file)[0]
                            output_filename = f"{folder_name}_{year}_{file_base}.parquet"
                            output_path = os.path.join(self.output_folder, output_filename)
                            df.to_parquet(output_path, index=False)
                            print(f"Saved cleaned file: {output_path}")
                        
                        # หลังจากประมวลผลสำเร็จ ให้ย้ายไฟล์ต้นฉบับไป complete folder
                        dest_path = os.path.join(self.complete_folder, file)
                        shutil.move(file_path, dest_path)
                        print(f"Moved original file to: {dest_path}")
                    else:
                        print(f"Processing failed for file: {file_path}")
        
        # ถ้า combine_by_year = True ให้รวมไฟล์ในแต่ละปีเข้าด้วยกันแล้วเซฟ
        if combine_by_year:
            for year, df_list in combined_data.items():
                combined_df = pd.concat(df_list, ignore_index=True)
                output_filename = f"{year}_combined.parquet"
                output_path = os.path.join(self.output_folder, output_filename)
                combined_df.to_parquet(output_path, index=False)
                print(f"Saved combined file for year {year}: {output_path}")

# ตัวอย่างการใช้งาน:
clean_process = CleanFile()
# สำหรับการประมวลผลไฟล์แต่ละไฟล์แยกกัน (combine_by_year=False)
clean_process.process_all_files(sheet_name='Db_FP', combine_by_year=False)
# หากต้องการรวมไฟล์เป็นไฟล์ต่อปีให้ใช้ combine_by_year=True
# clean_process.process_all_files(sheet_name='Db_FP', combine_by_year=True)