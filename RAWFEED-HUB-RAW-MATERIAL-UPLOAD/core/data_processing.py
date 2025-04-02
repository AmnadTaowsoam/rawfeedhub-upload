## core/data_processing.py

import pandas as pd
import json
from uuid import uuid4
from sqlalchemy.orm import Session
from core.db import Material, Plant, Vendor, Sample, AnalysisResult, MaterialSource, check_and_insert
from core.logging import get_logger

logger = get_logger(__name__)

class RMProcessor:
    def __init__(self):
        pass

    def load_data(self, path_file):
        """Load and rename data using column mapping."""
        try:
            rm_data = pd.read_parquet(path_file)
            logger.info("Data loaded and columns renamed successfully.")
            return rm_data
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def prepare_materials(self, data):
        """Prepare materials data for insertion."""
        try:
            materials = data[['material_code', 'material_description']].drop_duplicates()
            return [
                Material(
                    material_id=uuid4(),
                    material_code=row['material_code'],
                    material_description=row['material_description']
                )
                for _, row in materials.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for materials: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing materials: {e}")
            raise

    def prepare_plants(self, data):
        """Prepare plants data for insertion."""
        try:
            plants = data[['plant', 'plant_name']].drop_duplicates()
            return [
                Plant(
                    plant_id=uuid4(),
                    plant=row['plant'],
                    plant_name=row['plant_name']
                )
                for _, row in plants.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for plants: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing plants: {e}")
            raise

    def prepare_vendors(self, data):
        """Prepare vendors data for insertion."""
        try:
            vendors = data[['vendor_code', 'vendor_name']].drop_duplicates()
            return [
                Vendor(
                    vendor_id=uuid4(),
                    vendor_code=row['vendor_code'],
                    vendor_name=row['vendor_name']
                )
                for _, row in vendors.iterrows()
            ]
        except KeyError as e:
            logger.error(f"Missing column for vendors: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing vendors: {e}")
            raise

    def prepare_samples(self, data, materials_map, plants_map, vendors_map):
        """Prepare samples data for insertion."""
        try:
            samples = data[['sample_no', 'material_code', 'plant', 'vendor_code',
                            'inspection_lot', 'batch_no', 'material_doc', 'valuation_date']].drop_duplicates()
            return [
                Sample(
                    sample_id=uuid4(),
                    material_id=materials_map[row['material_code']],
                    plant_id=plants_map[row['plant']],
                    vendor_id=vendors_map[row['vendor_code']],
                    sample_no=str(row['sample_no']),  # แปลง sample_no เป็น string
                    inspection_lot=row['inspection_lot'],
                    batch_no=row['batch_no'],
                    material_doc=row['material_doc'],
                    valuation_date=row['valuation_date']
                )
                for _, row in samples.iterrows()
                if row['material_code'] in materials_map and row['plant'] in plants_map and row['vendor_code'] in vendors_map
            ]
        except KeyError as e:
            logger.error(f"Missing mapping for samples: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing samples: {e}")
            raise

    def prepare_analysis_results(self, data, samples_map):
        """Prepare analysis results data for insertion."""
        try:
            analysis_columns = [
                "moisture", "ash", "protein", "fat", "fiber", "p", "ca", "insoluble", "nacl",
                "ffa", "ua", "kohps", "brix", "pepsin", "pepsin0002", "ndf", "adf",
                "adl", "eth", "t_fat", "tvn", "nh3", "starch", "iv", "pv", "av",
                "totox", "p_anisidine", "xanthophyll", "ac_insol", "gluten", "sulfer", "sulfate"
            ]
            # Melt data and filter valid rows
            data_filtered = data[['sample_no', 'valuation_date'] + analysis_columns] \
                .melt(id_vars=['sample_no', 'valuation_date'], var_name='analysis_parameter', value_name='analysis_value') \
                .dropna(subset=['analysis_value', 'sample_no'])

            # แปลงค่า analysis_value เป็นตัวเลข และกรองค่าที่ไม่สามารถแปลงได้
            data_filtered['analysis_value'] = pd.to_numeric(data_filtered['analysis_value'], errors='coerce')
            data_filtered = data_filtered.dropna(subset=['analysis_value'])  # กรองค่า NaN ที่เกิดจากการแปลง

            logger.info(f"Filtered Analysis Data: {data_filtered.head(3)}")

            # Map sample_id from samples_map
            return [
                AnalysisResult(
                    result_id=uuid4(),
                    sample_id=samples_map[str(row['sample_no'])],  # ใช้ sample_id จาก samples_map
                    analysis_parameter=row['analysis_parameter'],
                    analysis_value=row['analysis_value'],
                    valuation_date=row['valuation_date']
                )
                for _, row in data_filtered.iterrows()
                if str(row['sample_no']) in samples_map  # ตรวจสอบว่า sample_no อยู่ใน samples_map
            ]
        except KeyError as e:
            logger.error(f"Missing column for analysis results: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing analysis results: {e}")
            raise

    def prepare_material_sources(self, data, samples_map):
        """Prepare material sources data for insertion."""
        try:
            # Filter out rows where all of the specified columns are NaN
            material_sources = data[['sample_no', 'valuation_date', 'plant_name', 'producer', 'country', 'original_batch']] \
                .drop_duplicates() \
                .loc[~data[['plant_name', 'producer', 'country', 'original_batch']].isna().all(axis=1)] \
                .dropna(subset=['sample_no', 'valuation_date'])  # Ensure sample_no and valuation_date are not NaN

            logger.info(f"Filtered Material Sources Data: {material_sources.head()}")

            # Map sample_id from samples_map and construct MaterialSource objects
            return [
                MaterialSource(
                    source_id=uuid4(),
                    sample_id=samples_map[str(row['sample_no'])],  # Use sample_id from samples_map
                    plant_origin=row['plant_name'] if not pd.isna(row['plant_name']) else None,
                    producer=row['producer'] if not pd.isna(row['producer']) else None,
                    country=row['country'] if not pd.isna(row['country']) else None,
                    original_batch=row['original_batch'] if not pd.isna(row['original_batch']) else None,
                    valuation_date=row['valuation_date']  # Ensure valuation_date is valid
                )
                for _, row in material_sources.iterrows()
                if str(row['sample_no']) in samples_map
            ]
        except KeyError as e:
            logger.error(f"Missing column for material sources: {e}")
            raise
        except Exception as e:
            logger.error(f"Error preparing material sources: {e}")
            raise

    def insert_to_db(self, session: Session, data):
        """Insert all prepared data into the database."""
        try:
            # Insert materials
            materials = self.prepare_materials(data)
            for material in materials:
                check_and_insert(session, Material, {"material_code": material.material_code}, vars(material))

            # Insert plants
            plants = self.prepare_plants(data)
            for plant in plants:
                check_and_insert(session, Plant, {"plant": plant.plant}, vars(plant))

            # Insert vendors
            vendors = self.prepare_vendors(data)
            for vendor in vendors:
                check_and_insert(session, Vendor, {"vendor_code": vendor.vendor_code}, vars(vendor))

            # Create mappings
            materials_map = {m.material_code: m.material_id for m in session.query(Material).all()}
            plants_map = {p.plant: p.plant_id for p in session.query(Plant).all()}
            vendors_map = {v.vendor_code: v.vendor_id for v in session.query(Vendor).all()}

            logger.info("Mappings created successfully.")

            # Insert samples
            samples = self.prepare_samples(data, materials_map, plants_map, vendors_map)
            for sample in samples:
                check_and_insert(session, Sample, {"sample_no": str(sample.sample_no), "valuation_date": sample.valuation_date}, vars(sample))

            # Create samples_map
            samples_map = {str(s.sample_no): s.sample_id for s in session.query(Sample).all()}  # Map sample_no to sample_id as string

            logger.info(f"Samples Map: {samples_map}")

            # Check for missing sample_no
            missing_samples = set(data['sample_no'].astype(str).unique()) - set(samples_map.keys())
            if missing_samples:
                logger.warning(f"Some sample_no are missing in samples_map: {missing_samples}")

            # Insert analysis results
            analysis_results = self.prepare_analysis_results(data, samples_map)
            logger.info(f"Number of Analysis Results to insert: {len(analysis_results)}")
            for result in analysis_results:
                check_and_insert(session, AnalysisResult, {"result_id": result.result_id}, vars(result))

            # Insert material sources
            material_sources = self.prepare_material_sources(data, samples_map)
            logger.info(f"Number of Material Sources to insert: {len(material_sources)}")
            for source in material_sources:
                check_and_insert(session, MaterialSource, {"source_id": source.source_id}, vars(source))

            session.commit()  # Commit the changes

            # Verify inserted data
            inserted_analysis_results = session.query(AnalysisResult).count()
            inserted_material_sources = session.query(MaterialSource).count()
            logger.info(f"Inserted {inserted_analysis_results} analysis results.")
            logger.info(f"Inserted {inserted_material_sources} material sources.")

            logger.info("All data inserted successfully.")
        except Exception as e:
            session.rollback()
            logger.error(f"Error during database insertion: {e}")
            raise

