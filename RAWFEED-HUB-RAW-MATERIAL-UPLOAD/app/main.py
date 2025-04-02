## app/main.py

import os
import shutil
import json
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from core.db import init_db, SessionLocal
from core.data_processing import RMProcessor
from core.clean_data import clean_and_process_files
from core.logging import get_logger

logger = get_logger(__name__)

# Paths ของไฟล์และโฟลเดอร์
COLUMN_MAPPING_PATH = './data/temp/master_data/column_mapping.json'
SCHEMA_FILE_PATH = './data/temp/master_data/schema_data_type.json'
RAW_DATA_FOLDER = './data/temp/raw_data/'
BUFFER_DATA_FOLDER = './data/temp/buffer_data/'
UPLOAD_COMPLETE_FOLDER = './data/temp/upload_complete/'

class BufferWatcherHandler(FileSystemEventHandler):
    """Class for handling file system events in buffer_data."""
    def __init__(self):
        super().__init__()

    def on_created(self, event):
        """Handle new files created in buffer_data."""
        if event.is_directory:
            return
        if event.src_path.endswith('.xlsx'):
            # Skip temporary files
            if os.path.basename(event.src_path).startswith("~$"):
                logger.info(f"Skipping temporary file: {event.src_path}")
                return

            logger.info(f"New file detected in buffer_data: {event.src_path}")
            try:
                # Ensure the upload_complete folder exists
                os.makedirs(UPLOAD_COMPLETE_FOLDER, exist_ok=True)

                # Retry logic for file access
                retries = 5
                retry_delay = 2  # seconds
                for attempt in range(retries):
                    try:
                        # Clean data and export to RAW_DATA_FOLDER
                        clean_and_process_files(
                            BUFFER_DATA_FOLDER, RAW_DATA_FOLDER, COLUMN_MAPPING_PATH, SCHEMA_FILE_PATH, date_column='DATE'
                        )
                        # Move processed file to UPLOAD_COMPLETE_FOLDER
                        shutil.move(event.src_path, os.path.join(UPLOAD_COMPLETE_FOLDER, os.path.basename(event.src_path)))
                        logger.info(f"File {event.src_path} processed and moved to {UPLOAD_COMPLETE_FOLDER}.")
                        break  # Exit retry loop on success
                    except PermissionError:
                        logger.warning(f"Permission denied for file {event.src_path}, retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{retries})")
                        time.sleep(retry_delay)  # Wait before retrying
                    except Exception as e:
                        logger.error(f"Error processing file {event.src_path}: {e}")
                        raise
                else:
                    logger.error(f"Failed to process file {event.src_path} after {retries} retries.")
            except Exception as e:
                logger.error(f"Unexpected error processing file {event.src_path}: {e}")

class RawDataWatcherHandler(FileSystemEventHandler):
    """Class สำหรับจัดการ Event ของ Watchdog ใน raw_data"""
    def __init__(self, session):
        super().__init__()
        self.session = session

    def on_created(self, event):
        """ตรวจจับเมื่อมีไฟล์ใหม่ใน raw_data"""
        if event.is_directory:
            return
        if event.src_path.endswith('.parquet'):
            logger.info(f"New file detected in raw_data: {event.src_path}")
            try:
                # ประมวลผลไฟล์ด้วย data_processing
                processor = RMProcessor()
                data = processor.load_data(event.src_path)
                processor.insert_to_db(self.session, data)

                # ย้ายไฟล์ไปยังโฟลเดอร์ upload_complete
                file_name = os.path.basename(event.src_path)
                completed_path = os.path.join(UPLOAD_COMPLETE_FOLDER, file_name)
                shutil.move(event.src_path, completed_path)
                logger.info(f"File {file_name} moved to {UPLOAD_COMPLETE_FOLDER}.")
            except Exception as e:
                logger.error(f"Error processing file in raw_data: {event.src_path}: {e}")

def main():
    # โหลด Column Mapping และ Schema
    try:
        with open(COLUMN_MAPPING_PATH, "r", encoding="utf-8") as f:
            column_mapping = json.load(f)
        with open(SCHEMA_FILE_PATH, "r", encoding="utf-8") as f:
            schema = json.load(f)
        logger.info("Column mapping and schema loaded successfully.")
    except Exception as e:
        logger.error(f"Error loading column mapping or schema: {e}")
        return

    # สร้างตารางในฐานข้อมูล
    try:
        init_db()
        logger.info("Database tables initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database tables: {e}")
        return

    # ตรวจสอบโฟลเดอร์ที่เกี่ยวข้อง
    for folder in [UPLOAD_COMPLETE_FOLDER, RAW_DATA_FOLDER, BUFFER_DATA_FOLDER]:
        if not os.path.exists(folder):
            os.makedirs(folder)
            logger.info(f"Folder {folder} created successfully.")

    # เริ่มต้น Session
    session = SessionLocal()

    # ใช้ Watchdog เพื่อ Monitor โฟลเดอร์
    buffer_event_handler = BufferWatcherHandler()
    raw_data_event_handler = RawDataWatcherHandler(session)

    observer = Observer()
    observer.schedule(buffer_event_handler, BUFFER_DATA_FOLDER, recursive=False)
    observer.schedule(raw_data_event_handler, RAW_DATA_FOLDER, recursive=False)

    try:
        logger.info(f"Starting to monitor folders: {BUFFER_DATA_FOLDER} and {RAW_DATA_FOLDER}")
        observer.start()
        while True:
            time.sleep(1)  # ให้โปรแกรมทำงานต่อเนื่อง
    except KeyboardInterrupt:
        observer.stop()
        logger.info("Folder monitoring stopped.")
    except Exception as e:
        logger.error(f"Error during folder monitoring: {e}")
    finally:
        observer.join()
        session.close()
        logger.info("Database session closed.")


if __name__ == "__main__":
    main()
