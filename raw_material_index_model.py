from datetime import datetime
from typing import Optional, List, Dict
from sqlalchemy import select, and_
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import UUID, NUMERIC
from sqlalchemy import Column, String, TIMESTAMP, Date, func
import uuid
from app.database import Base, get_db

class DictMixin:
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class Material(DictMixin, Base):
    __tablename__ = 'materials'
    __table_args__ = {'schema': 'raw_material', 'extend_existing': True}
    material_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    material_code = Column(String, unique=True, nullable=False)
    material_description = Column(String, nullable=False)
    
    @staticmethod
    def get_material_all() -> List[Dict]:
        try:
            with get_db() as session:
                query = select(Material)
                materials = session.execute(query).scalars().all()
                return [material.to_dict() for material in materials]
        except Exception as e:
            raise e
        
class Plant(DictMixin, Base):
    __tablename__ = 'plants'
    __table_args__ = {'schema': 'raw_material'}
    plant_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plant = Column(String, unique=True, nullable=False)
    plant_name = Column(String, nullable=False)
    
    @staticmethod
    def get_plant_all() -> List[Dict]:
        try:
            with get_db() as session:
                query = select(Plant)
                plants = session.execute(query).scalars().all()
                return [plant.to_dict() for plant in plants]
        except Exception as e:
            raise e

class Vendor(DictMixin, Base):
    __tablename__ = 'vendors'
    __table_args__ = {'schema': 'raw_material'}
    vendor_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    vendor_code = Column(String, unique=True, nullable=False)
    vendor_name = Column(String, nullable=False)
    
    @staticmethod
    def get_vendor_all() -> List[Dict]:
        try:
            with get_db() as session:
                query = select(Vendor)
                vendors = session.execute(query).scalars().all()
                return [vendor.to_dict() for vendor in vendors]
        except Exception as e:
            raise e
        
class AnalysisResult(DictMixin, Base):
    __tablename__ = 'analysis_results'
    __table_args__ = {'schema': 'raw_material'}
    result_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_id = Column(UUID(as_uuid=True), nullable=False)
    valuation_date = Column(Date, nullable=False)
    analysis_parameter = Column(String, nullable=False)
    analysis_value = Column(NUMERIC)
    created_at = Column(TIMESTAMP, server_default=func.now())
    
    @staticmethod
    def get_unique_analysis_parameters() -> List[str]:
        """
        ดึงค่า analysis_parameter ที่ไม่ซ้ำกันจากตาราง analysis_results
        """
        try:
            with get_db() as session:
                # สร้าง query โดยเลือกเฉพาะ analysis_parameter และใช้ distinct เพื่อให้ได้ค่า unique
                query = select(AnalysisResult.analysis_parameter).distinct()
                # ดึงผลลัพธ์ออกมาเป็น list ของค่า analysis_parameter
                unique_parameters = session.execute(query).scalars().all()
                return unique_parameters
        except Exception as e:
            raise e

class MaterialSource(DictMixin, Base):
    __tablename__ = 'material_sources'
    __table_args__ = {'schema': 'raw_material'}
    source_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_id = Column(UUID(as_uuid=True), nullable=False)
    valuation_date = Column(Date, nullable=False)
    plant_origin = Column(String, nullable=False)
    producer = Column(String, nullable=False)
    country = Column(String, nullable=False)
    original_batch = Column(String)
    created_at = Column(TIMESTAMP, server_default=func.now())
    
    @staticmethod
    def get_unique_producer_country() -> List[Dict]:
        """
        ดึงค่า producer และ country ที่ไม่ซ้ำกันจากตาราง material_sources
        โดยจะคืนค่าเป็น list ของ dictionary ที่มี key เป็น producer และ country
        """
        try:
            with get_db() as session:
                query = select(MaterialSource.producer, MaterialSource.country).distinct()
                results = session.execute(query).all()
                # แปลงผลลัพธ์จาก tuple เป็น dictionary
                unique_list = [{"producer": producer, "country": country} for producer, country in results]
                return unique_list
        except Exception as e:
            raise e