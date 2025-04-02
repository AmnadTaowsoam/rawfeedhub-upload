## app/database.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import Config

# Create SQLAlchemy Engine
engine = create_engine(Config.FULL_DATABASE_URL, pool_pre_ping=True)

# Create Session Factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base Model for SQLAlchemy
Base = declarative_base()

def get_db():
    """ Provide a database session and ensure proper cleanup. """
    db = None
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()