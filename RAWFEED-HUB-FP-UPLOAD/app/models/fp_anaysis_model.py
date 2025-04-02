import logging
import uuid
from sqlalchemy import Column, String, Date, Float, TIMESTAMP, text, PrimaryKeyConstraint, UniqueConstraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.orm import Session
from sqlalchemy.sql import func  # ตรวจสอบให้แน่ใจว่ามีการ import func ด้วย
from app.config import Config
from app.utils.logger import get_logger
from app.database import Base
from uuid import uuid4


logger = get_logger(__name__)

# -------------------------------
# Material Model
# -------------------------------
class Material(Base):
    __tablename__ = 'materials'
    __table_args__ = {'schema': 'finished_products'}

    material_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    material_code = Column(String, unique=True, nullable=False)
    material_description = Column(String, nullable=False)
    material_old_code = Column(String, unique=False)

    @classmethod
    def upsert(cls, session: Session, **kwargs):
        # ตรวจสอบว่า material_old_code มีใน kwargs หรือไม่
        m_old = kwargs.get("material_old_code")
        m_code = kwargs.get("material_code")
        if m_old:
            existing = session.query(cls).filter_by(material_old_code=m_old).first()
            if existing and existing.material_code != m_code:
                # ถ้า material_old_code ซ้ำกับ row อื่นที่มี material_code ต่างกัน,
                # ให้ลบค่านี้ออกจาก kwargs (หรือกำหนดเป็น None)
                kwargs.pop("material_old_code", None)
                # หรืออาจกำหนดเป็น None: kwargs["material_old_code"] = None
                # ขึ้นอยู่กับความต้องการของธุรกิจ

        stmt = insert(cls).values(**kwargs)
        # Exclude primary key and material_old_code from update fields
        update_dict = {
            c.name: stmt.excluded[c.name]
            for c in stmt.excluded
            if c.name not in ['material_id', 'material_old_code']
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['material_code'],
            set_=update_dict
        )
        session.execute(stmt)
        session.commit()

# -------------------------------
# Plants Model
# -------------------------------
class Plants(Base):
    __tablename__ = 'plants'
    __table_args__ = {'schema': 'finished_products'}

    plant_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plant = Column(String, unique=True, nullable=False)
    plant_name = Column(String, nullable=False)

    @classmethod
    def upsert(cls, session: Session, **kwargs):
        """
        Upsert for Plants based on unique plant.
        """
        stmt = insert(cls).values(**kwargs)
        update_dict = {c.name: stmt.excluded[c.name]
                       for c in stmt.excluded if c.name not in ['plant_id']}
        stmt = stmt.on_conflict_do_update(
            index_elements=['plant'],
            set_=update_dict
        )
        session.execute(stmt)
        session.commit()

# -------------------------------
# Formula Model (แก้ไขให้ตรงกับ schema ใหม่)
# -------------------------------
class Formula(Base):
    __tablename__ = 'formula'
    __table_args__ = (
        UniqueConstraint('formula_name', name='uq_formula_material_formula'),
        {'schema': 'finished_products'}
    )

    formula_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    formula_name = Column(String, nullable=False)

    @classmethod
    def upsert(cls, session: Session, **kwargs):
        """
        Upsert for Formula based on unique formula_name.
        """
        stmt = insert(cls).values(**kwargs)
        update_dict = {c.name: stmt.excluded[c.name]
                       for c in stmt.excluded if c.name not in ['formula_id']}
        stmt = stmt.on_conflict_do_update(
            index_elements=['formula_name'],
            set_=update_dict
        )
        session.execute(stmt)
        session.commit()

# -------------------------------
# Samples Model (แก้ไขให้เพิ่มคอลัมน์ formula_id)
# -------------------------------
class Samples(Base):
    __tablename__ = 'samples'
    __table_args__ = (
        PrimaryKeyConstraint('sample_id', 'manufacturing_date', name='pk_samples'),
        {'schema': 'finished_products'}
    )

    sample_id = Column(UUID(as_uuid=True), default=uuid4)
    material_id = Column(UUID(as_uuid=True), nullable=False)
    plant_id = Column(UUID(as_uuid=True), nullable=False)
    formula_id = Column(UUID(as_uuid=True), nullable=False)  # เพิ่มคอลัมน์ formula_id
    sample_no = Column(String, nullable=False)
    inspection_lot = Column(String)
    truck_no = Column(String)
    pallet_no = Column(String)
    batch_no = Column(String)
    manufacturing_date = Column(Date, nullable=False)
    bin_no = Column(String)
    load_time = Column(TIMESTAMP, nullable=False)
    validation_code = Column(String)
    remark = Column(String)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    @classmethod
    def upsert(cls, session: Session, **kwargs):
        """
        Upsert for Samples based on composite key (sample_id, manufacturing_date).
        Exclude created_at so that the DB default is used for new inserts and not updated on conflict.
        """
        stmt = insert(cls).values(**kwargs)
        update_dict = {
            c.name: stmt.excluded[c.name]
            for c in stmt.excluded
            if c.name not in ['sample_id', 'manufacturing_date', 'created_at']
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['sample_id', 'manufacturing_date'],
            set_=update_dict
        )
        session.execute(stmt)
        session.commit()

# -------------------------------
# AnalysisResults Model
# -------------------------------
class AnalysisResults(Base):
    __tablename__ = 'analysis_results'
    __table_args__ = (
        UniqueConstraint('sample_id', 'analysis_parameter', 'manufacturing_date', name='uq_analysis_sample_param_date'),
        {'schema': 'finished_products'}
    )

    result_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    sample_id = Column(UUID(as_uuid=True), nullable=False)
    manufacturing_date = Column(Date, nullable=False)
    analysis_parameter = Column(String, nullable=False)
    analysis_value = Column(Float)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

    __table_args__ = (
        ForeignKeyConstraint(
            ['sample_id', 'manufacturing_date'],
            ['finished_products.samples.sample_id', 'finished_products.samples.manufacturing_date']
        ),
        UniqueConstraint('sample_id', 'analysis_parameter', 'manufacturing_date', name='uq_analysis_sample_param_date'),
        {'schema': 'finished_products'}
    )

    @classmethod
    def upsert(cls, session: Session, **kwargs):
        """
        Upsert for AnalysisResults based on unique constraint (sample_id, analysis_parameter, manufacturing_date).
        Exclude created_at so that the DB default is preserved.
        """
        stmt = insert(cls).values(**kwargs)
        update_dict = {
            c.name: stmt.excluded[c.name]
            for c in stmt.excluded
            if c.name not in ['result_id', 'created_at']
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=['sample_id', 'analysis_parameter', 'manufacturing_date'],
            set_=update_dict
        )
        session.execute(stmt)
        session.commit()
