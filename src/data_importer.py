import os
from dotenv import load_dotenv
import pandas as pd
import re
from datetime import date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import numpy as np
import io

from src.utils.logger_config import AppLogger

logger = AppLogger().get_logger()

load_dotenv()
def clean_column_name(col):
    """Cleans column names by converting to lowercase and replacing non-alphanumeric with underscores."""
    cleaned = re.sub(r'_+', '_', re.sub(r'[^\w]+', '_', col.strip().lower())).strip('_')
    logger.debug(f"Cleaned column: Original='{col}', Cleaned='{cleaned}'")
    return cleaned


def download_and_extract(file_id, service):
    try:
        logger.info(f"Downloading file with ID: {file_id}")
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            logger.debug(f"Download progress: {int(status.progress() * 100)}%")

        fh.seek(0)
        df = pd.read_excel(fh, sheet_name=1, engine='openpyxl')
        logger.info(f"File downloaded and loaded into DataFrame with shape: {df.shape}")
        return df

    except ValueError as e:
        logger.exception("Sheet not found or cannot be read")
        return None
    except Exception as e:
        logger.exception("Unexpected error while downloading/extracting file")
        return None


def get_sheet_data_from_drive():
    try:
        logger.info("Starting Google Drive data import")

        # Setup credentials
        SERVICE_ACCOUNT_FILE = 'config/tracker-464109-993022ded408.json'
        SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        logger.debug("Google service account credentials loaded")

        service = build('drive', 'v3', credentials=creds)
        logger.debug("Google Drive API service built")

        # File ID from Google Drive
        FILE_ID = '1-mmzaZtq1t-EzqV0ZKlM6Zj9J22DCcJx'

        df = download_and_extract(FILE_ID, service)
        if df is None:
            logger.error("DataFrame is None — download failed or Excel unreadable")
            return None

        df = df.iloc[:, 1:]  # Drop index column
        logger.debug("Dropped first column (likely index)")

        df.columns = [clean_column_name(col) for col in df.columns]
        logger.debug(f"Cleaned columns: {df.columns.tolist()}")

        # Format relevant date columns
        date_columns = [
            'lead_generation_dt', 'ticket_assigned_dt',
            'assigned_dt_s', 'start_dt_s', 'closed_dt_s',
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ]

        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce', format='%d-%b-%y')
                df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None)
                logger.debug(f"Processed date column: {col}")

        # Backfill logic
        priority_map = {
            'assigned_dt_s': ['assigned_dt_s', 'start_dt_s', 'closed_dt_s', 'assigned_dt_c', 'start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'start_dt_s': ['start_dt_s', 'closed_dt_s', 'assigned_dt_c', 'start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'closed_dt_s': ['closed_dt_s', 'assigned_dt_c', 'start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'assigned_dt_c': ['assigned_dt_c', 'start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'start_dt_c': ['start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'closed_dt_c': ['closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'assigned_dt_qc': ['assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'start_dt_qc': ['start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'closed_date_qc': ['closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'assigned_dt_u': ['assigned_dt_u', 'start_dt_u', 'uploaded_date_u'],
            'start_dt_u': ['start_dt_u', 'uploaded_date_u'],
            'lead_generation_dt': ['lead_generation_dt', 'ticket_assigned_dt', 'assigned_dt_s', 'start_dt_s', 'closed_dt_s', 'assigned_dt_c', 'start_dt_c', 'closed_dt_c', 'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc', 'assigned_dt_u', 'start_dt_u', 'uploaded_date_u']
        }

        for target, sources in priority_map.items():
            available_sources = [col for col in sources if col in df.columns]
            if target in df.columns and len(available_sources) > 1:
                df[target] = df[available_sources].bfill(axis=1)[available_sources[0]]
                logger.debug(f"Backfilled '{target}' using: {available_sources}")

        # Status columns
        status_columns = ['ticket_status', 'status_s', 'status_c', 'status_qc', 'status_u']
        for col in status_columns:
            if col in df.columns:
                df[col] = df[col].fillna("In-progress")
                logger.debug(f"Filled nulls in status column: {col}")

        # Numeric columns to fill and convert
        columns = ['no_of_categories_s', 'no_of_categories_c', 'no_of_categories_u', 'no_of_products_c', 'no_of_products_u', 'no_of_products_s']
        for col in columns:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int).astype(object)
                logger.debug(f"Filled and casted numeric column: {col}")

        # Final cleanup
        df = df.where(pd.notnull(df), None)
        logger.info(f"DataFrame ready with shape: {df.shape}")

        csv_path = os.getenv("CSV_PATH")
        os.makedirs(csv_path, exist_ok=True)
        df.to_csv(os.path.join(csv_path, 'output.csv'), index=False)


        return df

    except Exception as e:
        logger.exception("Error occurred during sheet data import")
        return None
