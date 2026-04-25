"""Shared utilities for data collection scripts."""
import os
import time
import warnings
from datetime import timezone, timedelta

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

UTC8 = timezone(timedelta(hours=8))

CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def sheets_update_with_retry(sheets, range_, values, value_input='RAW', retries=5):
    for attempt in range(retries):
        try:
            sheets.update(
                spreadsheetId=SPREADSHEET_ID, range=range_,
                valueInputOption=value_input, body={'values': values}
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and ('429' in str(e) or 'Timeout' in str(e) or 'timed out' in str(e)):
                wait = 30 * (attempt + 1)
                print(f"  Sheets error ({e.__class__.__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def sheets_append_with_retry(sheets, range_, values, retries=5):
    for attempt in range(retries):
        try:
            sheets.append(
                spreadsheetId=SPREADSHEET_ID, range=range_,
                valueInputOption='RAW', insertDataOption='INSERT_ROWS',
                body={'values': values}
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and ('429' in str(e) or 'Timeout' in str(e) or 'timed out' in str(e)):
                wait = 30 * (attempt + 1)
                print(f"  Sheets error ({e.__class__.__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
