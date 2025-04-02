## services/data_processing.py

import os
import shutil
import pandas as pd
import json
from uuid import uuid4
from sqlalchemy.orm import Session
from sqlalchemy.inspection import inspect
from app.models.fp_anaysis_model import Material, Plants, Formula, Samples, AnalysisResults
from app.utils.logger import get_logger
from app.database import get_db  # ใช้ get_db() จาก database.py
from app.master_data.master_plants import master_plants
from app.master_data.master_column import numeric_cols

logger = get_logger(__name__)

def to_clean_dict(instance) -> dict:
    """Return a dictionary of column values from an SQLAlchemy model instance."""
    # ใช้ inspect เพื่อดึง column attributes
    data = {col.key: getattr(instance, col.key, None) for col in inspect(instance).mapper.column_attrs}
    # ถ้า created_at เป็น None ให้ลบ key นี้ออก เพื่อให้ฐานข้อมูลใช้ค่า default
    if data.get("created_at") is None:
        data.pop("created_at", None)
    return data

def check_and_insert(session: Session, model, unique_filter: dict, data_obj):
    """
    Helper function to perform an upsert operation using the model's upsert method.
    This assumes the model class has an upsert() class method.
    """
    try:
        data = to_clean_dict(data_obj)
        model.upsert(session, **data)
    except Exception as e:
        logger.error(f"Error upserting {model.__name__} with filter {unique_filter}: {e}")
        raise

class FPProcessor:
    def __init__(self):
        pass

    def load_data(self, path_file):
        """Load data from a parquet file."""
        try:
            rm_data = pd.read_parquet(path_file)
            logger.info("Data loaded successfully.")
            logger.info(f"Columns in loaded data: {rm_data.columns.tolist()}")
            return rm_data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def prepare_materials(self, data: pd.DataFrame):
        """Prepare materials data for insertion."""
        try:
            # คาดว่า DataFrame มีคอลัมน์ material_code, material_description, material_old_code
            materials_df = data[['material_code', 'material_description', 'material_old_code']].drop_duplicates()
            logger.info(f"prepare_materials: {materials_df.shape[0]} records found.")
            
            # แปลงค่า material_old_code ที่เป็น empty string ให้เป็น None
            materials_df['material_old_code'] = materials_df['material_old_code'].apply(lambda x: x if x.strip() != "" else None)
            
            return [
                Material(
                    material_id=uuid4(),
                    material_code=row['material_code'],
                    material_description=row['material_description'],
                    material_old_code=row['material_old_code']
                )
                for _, row in materials_df.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for materials: {e}. Available columns: {data.columns.tolist()}")
            raise
        except Exception as e:
            logger.error(f"Error preparing materials: {e}")
            raise

    def prepare_plants(self, data: pd.DataFrame):
        """Prepare plants data for insertion."""
        try:
            # ตรวจสอบว่ามีคอลัมน์ plant_name อยู่หรือไม่
            # ถ้าไม่มี ให้ใช้ mapping จาก master_plant เพื่อกำหนดค่า plant_name โดยใช้ค่าในคอลัมน์ plant เป็น key
            if 'plant_name' not in data.columns:
                logger.warning("'plant_name' column not found. Creating from master_plant mapping.")
                data['plant_name'] = data['plant'].map(master_plants).fillna("")
            else:
                # ถ้ามีอยู่แล้ว สามารถเลือกที่จะ override ด้วย master_plant mapping ได้
                data['plant_name'] = data['plant'].map(master_plants).fillna(data['plant_name'])
                
            plants_df = data[['plant', 'plant_name']].drop_duplicates()
            logger.info(f"prepare_plants: {plants_df.shape[0]} records found. Columns: {plants_df.columns.tolist()}")
            return [
                Plants(
                    plant_id=uuid4(),
                    plant=row['plant'],
                    plant_name=row['plant_name']
                )
                for _, row in plants_df.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for plants: {e}. Available columns: {data.columns.tolist()}")
            raise
        except Exception as e:
            logger.error(f"Error preparing plants: {e}")
            raise

    def prepare_formula(self, data: pd.DataFrame):
        """
        Prepare formula data for insertion.
        คาดว่า DataFrame มีคอลัมน์ formula_name
        """
        try:
            if 'formula_name' not in data.columns:
                logger.warning("'formula_name' column not found. Skipping formula preparation.")
                return []
            formulas_df = data[['formula_name']].drop_duplicates()
            logger.info(f"prepare_formula: {formulas_df.shape[0]} records found.")
            return [
                Formula(
                    formula_id=uuid4(),
                    formula_name=row['formula_name']
                )
                for _, row in formulas_df.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for formula: {e}. Available columns: {data.columns.tolist()}")
            raise
        except Exception as e:
            logger.error(f"Error preparing formula: {e}")
            raise

    def prepare_samples(self, data: pd.DataFrame, materials_map: dict, plants_map: dict, formulas_map: dict):
        """
        Prepare samples data for insertion.
        คาดว่า DataFrame มีคอลัมน์:
        - sample_no, material_code, plant, formula_name, inspection_lot, truck_no, pallet_no,
            batch_no, manufacturing_date, bin_no, load_time, remark
        """
        try:
            required_cols = ['sample_no', 'material_code', 'plant', 'formula_name', 
                            'manufacturing_date', 'load_time']
            samples_df = data[required_cols + ['inspection_lot', 'truck_no', 'pallet_no', 'batch_no', 'bin_no', 'validation_code', 'remark']].drop_duplicates()
            logger.info(f"prepare_samples: {samples_df.shape[0]} records found.")
            return [
                Samples(
                    sample_id=uuid4(),
                    material_id=materials_map[row['material_code']],
                    plant_id=plants_map[row['plant']],
                    formula_id=formulas_map[row['formula_name']],
                    sample_no=str(row['sample_no']),
                    inspection_lot=row.get('inspection_lot'),
                    truck_no=row.get('truck_no'),
                    pallet_no=row.get('pallet_no'),
                    batch_no=row.get('batch_no'),
                    manufacturing_date=row['manufacturing_date'],
                    bin_no=row.get('bin_no'),
                    load_time=row['load_time'],
                    validation_code=row['validation_code'],
                    remark=row.get('remark')
                )
                for _, row in samples_df.iterrows()
                if (row['material_code'] in materials_map and 
                    row['plant'] in plants_map and 
                    row['formula_name'] in formulas_map)
            ]
        except KeyError as e:
            logger.error(f"Missing mapping or column for samples: {e}. Available columns: {data.columns.tolist()}")
            raise
        except Exception as e:
            logger.error(f"Error preparing samples: {e}")
            raise

    def prepare_analysis_results(self, data: pd.DataFrame, samples_map: dict):
        """
        Prepare analysis results data for insertion.
        คาดว่า DataFrame มีคอลัมน์ sample_no, manufacturing_date และคอลัมน์ผลวิเคราะห์อื่น ๆ
        เช่น moisture, ash, protein, fat, fiber, p, ca, insoluble, nacl, ... เป็นต้น
        """
        try:
            # เปลี่ยนรูป DataFrame ให้อยู่ในรูป long format
            data_filtered = data[['sample_no', 'manufacturing_date'] + numeric_cols] \
                .melt(id_vars=['sample_no', 'manufacturing_date'], 
                    var_name='analysis_parameter', value_name='analysis_value') \
                .dropna(subset=['analysis_value', 'sample_no', 'manufacturing_date'])
            # แปลงค่า analysis_value เป็นตัวเลข
            data_filtered['analysis_value'] = pd.to_numeric(data_filtered['analysis_value'], errors='coerce')
            data_filtered = data_filtered.dropna(subset=['analysis_value'])

            logger.info(f"prepare_analysis_results: Filtered data shape {data_filtered.shape}")
            logger.info(f"Sample of filtered analysis results:\n{data_filtered.head(3)}")

            return [
                AnalysisResults(
                    result_id=uuid4(),
                    sample_id=samples_map[str(row['sample_no'])],
                    manufacturing_date=row['manufacturing_date'],
                    analysis_parameter=row['analysis_parameter'],
                    analysis_value=row['analysis_value']
                    
                )
                for _, row in data_filtered.iterrows()
                if str(row['sample_no']) in samples_map
            ]
        except KeyError as e:
            logger.error(f"Missing column for analysis results: {e}. Available columns: {data.columns.tolist()}")
            raise
        except Exception as e:
            logger.error(f"Error preparing analysis results: {e}")
            raise   

    def update_to_db(self, data: pd.DataFrame):
        session = next(get_db())
        try:
            # Insert materials
            materials = self.prepare_materials(data)
            logger.info(f"Materials prepared: {len(materials)} records.")
            for material in materials:
                check_and_insert(session, Material, {"material_code": material.material_code}, material)

            # Insert plants
            plants = self.prepare_plants(data)
            logger.info(f"Plants prepared: {len(plants)} records.")
            for plant in plants:
                check_and_insert(session, Plants, {"plant": plant.plant}, plant)

            # Create mappings for materials and plants
            materials_map = {m.material_code: m.material_id for m in session.query(Material).all()}
            plants_map = {p.plant: p.plant_id for p in session.query(Plants).all()}
            logger.info("Mappings for materials and plants created successfully.")

            # Insert formula records
            formulas = self.prepare_formula(data)
            logger.info(f"Formulas prepared: {len(formulas)} records.")
            for formula in formulas:
                check_and_insert(session, Formula, {"formula_name": formula.formula_name}, formula)
            # Create mapping for formulas: formula_name -> formula_id
            formulas_map = {f.formula_name: f.formula_id for f in session.query(Formula).all()}

            # Insert samples
            samples = self.prepare_samples(data, materials_map, plants_map, formulas_map)
            logger.info(f"Samples prepared: {len(samples)} records.")
            for sample in samples:
                check_and_insert(
                    session, Samples, 
                    {"sample_no": str(sample.sample_no), "manufacturing_date": sample.manufacturing_date}, 
                    sample
                )

            # Insert analysis results
            analysis_results = self.prepare_analysis_results(data, {str(s.sample_no): s.sample_id for s in session.query(Samples).all()})
            logger.info(f"Analysis results prepared: {len(analysis_results)} records.")
            for result in analysis_results:
                check_and_insert(
                    session, AnalysisResults, 
                    {
                        "sample_id": result.sample_id, 
                        "analysis_parameter": result.analysis_parameter, 
                        "manufacturing_date": result.manufacturing_date
                    },
                    result
                )

            session.commit()
            logger.info("All data inserted successfully.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error during database insertion: {e}")
            raise
        finally:
            session.close()
            
    def process_all_files(self, folder_path: str, complete_folder: str):
        # สร้างโฟลเดอร์ complete หากยังไม่มี
        os.makedirs(complete_folder, exist_ok=True)
        processor = FPProcessor()
        
        # วนลูปอ่านไฟล์ที่มีนามสกุล .parquet ใน folder_path
        for filename in os.listdir(folder_path):
            if filename.lower().endswith(".parquet"):
                file_path = os.path.join(folder_path, filename)
                logger.info(f"Processing file: {file_path}")
                try:
                    data = processor.load_data(file_path)
                    processor.update_to_db(data)
                    # หากการอัปโหลดสำเร็จ ให้ย้ายไฟล์ไปยัง complete_folder
                    dest_path = os.path.join(complete_folder, filename)
                    shutil.move(file_path, dest_path)
                    logger.info(f"File {filename} processed and moved to {complete_folder}")
                except Exception as e:
                    logger.error(f"Error processing file {filename}: {e}")
     
# ตัวอย่างการเรียกใช้งาน
processor = FPProcessor()
source_folder = "./data/clean_file"
complete_folder = "./data/upload_complete"
processor.process_all_files(source_folder, complete_folder)