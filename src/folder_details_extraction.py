import os
import re
import pandas as pd
from datetime import datetime
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# === CONFIG ===
BUCKET_NAME = os.getenv('BUCKET_NAME')
PREFIX = os.getenv('BUCKET_PATH')  # Folder path inside bucket
CREDENTIALS_PATH = 'config/bluetdev-54bdd1028219.json'  # Path to your service account JSON

# === Load credentials and initialize GCS client ===
creds = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
client = storage.Client(credentials=creds)
bucket = client.bucket(BUCKET_NAME)

def normalize_name(name):
    """Remove anything in parentheses like ' (1)' from the name."""
    return re.sub(r'\s*\(.*?\)', '', name).strip()

# === Clean filename helper ===
def extract_info_from_filename(filename):
    """Extract file_id, person1, person2 from filename"""
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
    
        # Normalize names
    person1 = normalize_name(person1)
    person2 = normalize_name(person2)
    return file_name, person1, person2

# === Process metadata ===
def list_xlsx_metadata(bucket, prefix):
    blobs = bucket.list_blobs(prefix=prefix)
    data = []

    for blob in blobs:
        if blob.name.endswith('.xlsx'):
            file_name = blob.name.split('/')[-1]
            base_name, person1, person2 = extract_info_from_filename(file_name)
            mod_date = blob.updated.strftime('%d-%b-%y')
            create_date = blob.time_created.strftime('%d-%b-%y')
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

    return pd.DataFrame(data)

def details_extractions():
    return list_xlsx_metadata(bucket, PREFIX)