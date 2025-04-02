# config.py

from dotenv import load_dotenv
import os
from pathlib import Path

# โหลด environment variables จากไฟล์ .env
load_dotenv()

# กำหนดค่าโดยใช้ environment variables ที่โหลดมา
class Config:
    
    # database configurations
    DB_NAME= os.getenv("DB_NAME", "")
    DB_USERNAME= os.getenv("DB_USERNAME", "")
    DB_PASSWORD= os.getenv("DB_PASSWORD", "")
    DB_HOST= os.getenv("DB_HOST", "")
    DB_PORT= int(os.getenv("DB_PORT","5432"))
    DB_SCHEMA= os.getenv("DB_SCHEMA", "")

    # CORS Settings
    CORS_ALLOWED_ORIGINS = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:6008").split(",")
    CORS_ALLOW_CREDENTIALS = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"
    CORS_ALLOW_METHODS = os.getenv("CORS_ALLOW_METHODS", "GET,POST,PUT,DELETE").split(",")
    CORS_ALLOW_HEADERS = os.getenv("CORS_ALLOW_HEADERS", "Authorization,Content-Type,Accept").split(",")
    
    # Additional settings
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY")
    SECRET_KEY: str = os.getenv("SECRET_KEY")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", 10))
    TOKEN_EXPIRE_MINUTES: int = int(os.getenv("TOKEN_EXPIRE_MINUTES", 30))
    READ_TIMEOUT = int(os.getenv("READ_TIMEOUT", 30))
    ALGORITHM = os.getenv("ALGORITHM", "HS256")

    ## api key
    API_KEY: str = os.getenv("API_KEY")