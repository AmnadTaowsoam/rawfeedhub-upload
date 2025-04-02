## utils/config.py

from dotenv import load_dotenv
import os
from pathlib import Path

# โหลด .env จากระดับโฟลเดอร์ที่อยู่ 2 ระดับเหนือ config.py
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")
print(f"Loaded .env path: {Path(__file__).resolve().parents[1] / '.env'}")


class Config:
    # Database Config
    DB_NAME: str = os.getenv("DB_NAME", "")
    DB_USERNAME: str = os.getenv("DB_USERNAME", "")  # เปลี่ยนจาก DB_USER เป็น DB_USERNAME
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "")
    DB_HOST: str = os.getenv("DB_HOST", "host.docker.internal")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    
    # Full Database URL
    FULL_DATABASE_URL: str = f"postgresql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    
    # CORS Settings
    CORS_ALLOWED_ORIGINS: list = os.getenv("CORS_ALLOWED_ORIGINS", "").split(",")
    CORS_ALLOW_CREDENTIALS: bool = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"
    CORS_ALLOW_METHODS: list = os.getenv("CORS_ALLOW_METHODS", "GET,POST,PUT,DELETE").split(",")
    CORS_ALLOW_HEADERS: list = os.getenv("CORS_ALLOW_HEADERS", "Authorization,Content-Type,Accept").split(",")
    
    # API Key and Secrets
    API_KEY: str = os.getenv("API_KEY", "default_api_key")
    JWT_SECRET_KEY: str = os.getenv("JWT_SECRET_KEY", "")
    SECRET_KEY: str = os.getenv("SECRET_KEY", "")
    ALGORITHM: str = os.getenv("ALGORITHM", "HS256")
    
    # Logging and Timeouts
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "DEBUG")
    CONNECTION_TIMEOUT: int = int(os.getenv("CONNECTION_TIMEOUT", "10"))
    READ_TIMEOUT: int = int(os.getenv("READ_TIMEOUT", "30"))
    TOKEN_EXPIRE_MINUTES: int = int(os.getenv("TOKEN_EXPIRE_MINUTES", "30"))