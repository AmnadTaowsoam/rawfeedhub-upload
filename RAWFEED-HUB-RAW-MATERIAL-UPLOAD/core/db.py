## core/db.py

import logging
import uuid
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, Float, ForeignKey, TIMESTAMP, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy.inspection import inspect
from config import Config
from core.logging import get_logger
from contextlib import contextmanager

logger = get_logger(__name__)

# Database connection setup
DATABASE_URL = f"postgresql://{Config.DB_USERNAME}:{Config.DB_PASSWORD}@{Config.DB_HOST}:{Config.DB_PORT}/{Config.DB_NAME}"

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Models
class Material(Base):
    __tablename__ = 'materials'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    material_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    material_code = Column(String, unique=True, nullable=False)
    material_description = Column(String, nullable=False)

class Plant(Base):
    __tablename__ = 'plants'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    plant_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plant = Column(String, unique=True, nullable=False)
    plant_name = Column(String, nullable=False)

class Vendor(Base):
    __tablename__ = 'vendors'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    vendor_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vendor_code = Column(String, unique=True, nullable=False)
    vendor_name = Column(String, nullable=False)

class Sample(Base):
    __tablename__ = 'samples'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    sample_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    material_id = Column(UUID(as_uuid=True), ForeignKey(f"{Config.DB_SCHEMA}.materials.material_id"), nullable=False)
    plant_id = Column(UUID(as_uuid=True), ForeignKey(f"{Config.DB_SCHEMA}.plants.plant_id"), nullable=False)
    vendor_id = Column(UUID(as_uuid=True), ForeignKey(f"{Config.DB_SCHEMA}.vendors.vendor_id"), nullable=False)
    sample_no = Column(String, nullable=False)
    inspection_lot = Column(String)
    valuation_date = Column(Date, nullable=False)
    batch_no = Column(String)
    material_doc = Column(String)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

class AnalysisResult(Base):
    __tablename__ = 'analysis_results'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    result_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_id = Column(UUID(as_uuid=True), ForeignKey(f"{Config.DB_SCHEMA}.samples.sample_id"), nullable=False)
    analysis_parameter = Column(String, nullable=False)
    analysis_value = Column(Float)
    valuation_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

class MaterialSource(Base):
    __tablename__ = 'material_sources'
    __table_args__ = {'schema': Config.DB_SCHEMA}
    source_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_id = Column(UUID(as_uuid=True), ForeignKey(f"{Config.DB_SCHEMA}.samples.sample_id"), nullable=False)
    plant_origin = Column(String, nullable=False)
    producer = Column(String, nullable=False)
    country = Column(String, nullable=False)
    original_batch = Column(String)
    valuation_date = Column(Date, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))

# Database initialization
def init_db() -> None:
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully.")
    except Exception as e:
        logger.error(f"Error creating database tables: {e}")

# Utility function for inserting or updating data
def check_and_insert(
    session: Session, model: Base, filters: dict, values: dict, update_on_conflict: bool = False
) -> None:
    try:
        # Filter out unconsumed columns like _sa_instance_state
        column_keys = {c.key for c in inspect(model).mapper.column_attrs}
        filtered_values = {key: values[key] for key in values if key in column_keys}

        stmt = insert(model).values(**filtered_values)

        if update_on_conflict:
            update_values = {key: stmt.excluded[key] for key in filtered_values if key not in filters}
            stmt = stmt.on_conflict_do_update(index_elements=list(filters.keys()), set_=update_values)
        else:
            stmt = stmt.on_conflict_do_nothing()

        session.execute(stmt)
        session.commit()
        logger.info(f"Record inserted/updated in {model.__tablename__}: {filtered_values}")
    except IntegrityError as e:
        session.rollback()
        logger.error(f"IntegrityError in {model.__tablename__}: {e}")
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error in {model.__tablename__}: {e}")

# Context manager for session handling
@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

# Insert data function
def insert_data(session: Session) -> None:
    try:
        entities = [
            (Material, {"material_code": "MAT001"}, {
                "material_code": "MAT001", "material_description": "Steel Rod"
            }),
            (Plant, {"plant": "Plant1"}, {"plant": "Plant1", "plant_name": "Main Plant"}),
            (Vendor, {"vendor_code": "V001"}, {"vendor_code": "V001", "vendor_name": "Vendor A"})
        ]

        for model, filters, values in entities:
            check_and_insert(session, model, filters, values)

        material = session.query(Material).filter_by(material_code="MAT001").one()
        plant = session.query(Plant).filter_by(plant="Plant1").one()
        vendor = session.query(Vendor).filter_by(vendor_code="V001").one()

        sample_values = {
            "material_id": material.material_id,
            "plant_id": plant.plant_id,
            "vendor_id": vendor.vendor_id,
            "sample_no": "S001",
            "valuation_date": "2024-01-01",
            "inspection_lot": "Lot001",
            "batch_no": "Batch001",
            "material_doc": "Doc001"
        }
        check_and_insert(session, Sample, {"sample_no": "S001", "valuation_date": "2024-01-01"}, sample_values)

        sample = session.query(Sample).filter_by(sample_no="S001", valuation_date="2024-01-01").one()

        analysis_values = {
            "sample_id": sample.sample_id,
            "analysis_parameter": "Moisture",
            "analysis_value": 10.5,
            "valuation_date": "2024-01-01"
        }
        check_and_insert(session, AnalysisResult, {"sample_id": sample.sample_id, "analysis_parameter": "Moisture"}, analysis_values)

        source_values = {
            "sample_id": sample.sample_id,
            "valuation_date": "2024-01-01",
            "plant_origin": "Origin Plant",
            "producer": "Producer Name",
            "country": "Country",
            "original_batch": "Batch001"
        }
        check_and_insert(session, MaterialSource, {"sample_id": sample.sample_id, "valuation_date": "2024-01-01"}, source_values)

        logger.info("Data insertion completed successfully.")
    except Exception as e:
        logger.error(f"Error during data insertion: {e}")
        raise