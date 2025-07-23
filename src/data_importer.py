# import pandas as pd
# import re
# from datetime import datetime, date
# import gspread
# from oauth2client.service_account import ServiceAccountCredentials


# def clean_column_name(col):
#     """Cleans column names by converting to lowercase and replacing non-alphanumeric with underscores."""
#     col = col.strip().lower()
#     col = re.sub(r'[^\w]+', '_', col)
#     col = re.sub(r'_+', '_', col)
#     return col.strip('_')


# def get_sheet_data():
#     # Google Sheets API setup
#     scope = [
#         "https://spreadsheets.google.com/feeds",
#         "https://www.googleapis.com/auth/drive"
#     ]
#     creds = ServiceAccountCredentials.from_json_keyfile_name("config/tracker-464109-993022ded408.json", scope)
#     client = gspread.authorize(creds)

#     # Sheet setup
#     # sheet = client.open_by_key('1twz3NjPnpg-q3cuYYDaSpDXECrWsIxypemYTOYEl8xQ')
#     sheet = client.open_by_key('1-mmzaZtq1t-EzqV0ZKlM6Zj9J22DCcJx')
#     option = 1  # 1 for batch, 3 for delta
#     worksheet = sheet.get_worksheet(option)

#     # Load data
#     data = worksheet.get_all_records()
#     df = pd.DataFrame(data)
#     df = df.iloc[:, 1:]  # Drop first column (index column)

#     df.columns = [clean_column_name(col) for col in df.columns]

#     if option != 1:
#         worksheet.resize(rows=1)  # Clear old rows in delta mode

#     # Relevant date columns
#     date_columns = [
#         'lead_generation_dt', 'ticket_assigned_dt',
#         'assigned_dt_s', 'start_dt_s', 'closed_dt_s',
#         'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
#         'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
#         'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
#     ]

#     # Step 1: Convert all date columns to datetime
#     for col in date_columns:
#         df[col] = pd.to_datetime(df[col], errors='coerce', format='%d-%b-%y')

#     # Step 2: Fill missing assigned/start dates from related end dates
#     fill_pairs = [
#         ('assigned_dt_s', 'closed_dt_s'),
#         ('start_dt_s', 'closed_dt_s'),
#         ('assigned_dt_c', 'closed_dt_c'),
#         ('start_dt_c', 'closed_dt_c'),
#         ('assigned_dt_qc', 'closed_date_qc'),
#         ('start_dt_qc', 'closed_date_qc'),
#         ('assigned_dt_u', 'uploaded_date_u'),
#         ('start_dt_u', 'uploaded_date_u'),
#         ('lead_generation_dt', 'ticket_assigned_dt')
#     ]

#     for target, source in fill_pairs:
#         df[target] = df[target].fillna(df[source])

#     # Step 3: Replace remaining nulls with future date (for logic consistency)
#     future_date = date(9999, 12, 31)

#     for col in date_columns:
#         df[col] = df[col].apply(
#             lambda x: x.strftime('%d-%m-%Y') if pd.notna(x) else '31-12-9999'
#         )
#     status_columns = ['ticket_status', 'status_s', 'status_c', 'status_qc', 'status_u']
#     for col in status_columns:
#         df[col] = df[col].fillna("In-progress")
#     print(df.head())
#     # return df

import os
import pandas as pd
import re
from datetime import date
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import numpy as np
import io


def clean_column_name(col):
    """Cleans column names by converting to lowercase and replacing non-alphanumeric with underscores."""
    col = col.strip().lower()
    col = re.sub(r'[^\w]+', '_', col)
    col = re.sub(r'_+', '_', col)
    return col.strip('_')


def download_and_extract(file_id, service):
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        df = pd.read_excel(fh, sheet_name=1, engine='openpyxl')
        return df

    except ValueError as e:
        print(f"Sheet not found in file: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None
    

def get_sheet_data_from_drive():
    # Google Drive API setup
    SERVICE_ACCOUNT_FILE = 'config/tracker-464109-993022ded408.json'
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly']

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    service = build('drive', 'v3', credentials=creds)

    # Replace with your actual file ID from Google Drive
    FILE_ID = '1-mmzaZtq1t-EzqV0ZKlM6Zj9J22DCcJx'

    df = download_and_extract(FILE_ID, service)
    df = df.iloc[:, 1:]  # Drop first column (index column)
    df.columns = [clean_column_name(col) for col in df.columns]

    # Relevant date columns
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
            df[col] = df[col].apply(
               lambda x: x.strftime('%d-%m-%Y') if pd.notna(x) else None
            )
    # Define fallback chains for each target column
    priority_map = {
        'assigned_dt_s': [
            'assigned_dt_s', 'start_dt_s', 'closed_dt_s',
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'start_dt_s': [
            'start_dt_s', 'closed_dt_s',
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'closed_dt_s': [
            'closed_dt_s',
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'assigned_dt_c': [
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'start_dt_c': [
            'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'closed_dt_c': [
            'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'assigned_dt_qc': [
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'start_dt_qc': [
            'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'closed_date_qc': [
            'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'assigned_dt_u': [
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ],
        'start_dt_u': [
            'start_dt_u', 'uploaded_date_u'
        ],
        'lead_generation_dt': [
            'lead_generation_dt', 'ticket_assigned_dt',
            'assigned_dt_s', 'start_dt_s', 'closed_dt_s',
            'assigned_dt_c', 'start_dt_c', 'closed_dt_c',
            'assigned_dt_qc', 'start_dt_qc', 'closed_date_qc',
            'assigned_dt_u', 'start_dt_u', 'uploaded_date_u'
        ]
    }


    # Efficient one-pass filling using backfill logic
    for target, sources in priority_map.items():
        # Only process if the target exists in DataFrame
        available_sources = [col for col in sources if col in df.columns]
        if target in df.columns and len(available_sources) > 1:
            df[target] = df[available_sources].bfill(axis=1)[available_sources[0]]

    # # Replace remaining nulls with future date
    # for col in date_columns:
    #     if col in df.columns:
    #         df[col] = df[col].apply(
    #             lambda x: x.strftime('%d-%m-%Y') if pd.notna(x) else '31-12-9999'
    #         )

    status_columns = ['ticket_status', 'status_s', 'status_c', 'status_qc', 'status_u']
    for col in status_columns:
        if col in df.columns:
            df[col] = df[col].fillna("In-progress")

    columns = ['no_of_categories_s', 'no_of_categories_c', 'no_of_categories_u', 'no_of_products_c', 'no_of_products_u', 'no_of_products_s']
    for col in columns:
      df[col] = df[col].fillna(0).astype('int').astype('object')


    # print(df.head())
    return df

# get_excel_data_from_drive()
