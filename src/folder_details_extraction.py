import os
import re
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from google.oauth2 import service_account
import psycopg2
from dotenv import load_dotenv
from src.utils.logger_config import AppLogger  # Adjust path
from src.utils.etl_updater import get_etl_metadata, update_etl_metadata
from src.utils.db_connection import get_connection

logger = AppLogger().get_logger()
load_dotenv()

# === CONFIG ===
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = os.getenv('BUCKET_PATH')
CREDENTIALS_PATH = os.getenv('BUCKET_CREDENTIALS_PATH')

# === GCS Setup ===
try:
    creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    client = storage.Client(credentials=creds)
    bucket = client.bucket(BUCKET_NAME)
    logger.info("GCS client initialized")
except Exception as e:
    logger.exception("Failed to initialize GCS client")


# === Filename Parser ===
def normalize_name(name):
    return re.sub(r'\s*\(.*?\)', '', name).strip()


def extract_info_from_filename(filename):
    try:
        name_without_ext = filename.split('/')[-1].replace('.xlsx', '')
        parts = name_without_ext.split('$#$')

        if len(parts) == 3:
            file_name, person1, person2 = parts
        elif len(parts) == 2:
            file_name, person1 = parts
            person2 = "Backend"
        else:
            file_name = parts[0]
            person1 = person2 = "Backend"

        return file_name, normalize_name(person1), normalize_name(person2)

    except Exception as e:
        logger.exception(f"Error parsing filename: {filename}")
        return "unknown", "Backend", "Backend"


# === Main Extraction Function ===
def extract_delta_xlsx_metadata():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            last_loaded_at = get_etl_metadata(cur, source_table='file_tracker')
            logger.info(f"Last loaded timestamp: {last_loaded_at}")

        blobs = bucket.list_blobs(prefix=PREFIX)
        data = []
        latest_update = last_loaded_at

        for blob in blobs:
            if not blob.name.endswith('.xlsx'):
                continue

            if last_loaded_at and blob.updated <= last_loaded_at:
                continue  # Skip already processed

            file_name = blob.name.split('/')[-1]
            base_name, person1, person2 = extract_info_from_filename(file_name)

            mod_date = blob.updated.strftime('%Y-%m-%d %H:%M:%S')
            create_date = blob.time_created.strftime('%Y-%m-%d %H:%M:%S')
            status = blob.name.split('/')[-2].split('_')[0] if '/' in blob.name else 'Unknown'
            file_id = f"{base_name}_{mod_date}"

            data.append({
                'file_id': file_id,
                'filename': base_name,
                'qc_done_by': person1,
                'uploaded_by': person2,
                'create_date': create_date,
                'modified_date': mod_date,
                'status': status
            })

            if latest_update is None or blob.updated > latest_update:
                latest_update = blob.updated

        df = pd.DataFrame(data)
        logger.info(f"Delta extracted: {df.shape[0]} rows")

        # Update metadata only if new files found
        if latest_update and not df.empty:
            with conn.cursor() as cur:
                update_etl_metadata(cur, source_table='file_tracker')
            conn.commit()

        return df

    except Exception as e:
        logger.exception("Delta extraction failed")
        return pd.DataFrame()
    finally:
        conn.close()

def details_extractions():
    return extract_delta_xlsx_metadata()

