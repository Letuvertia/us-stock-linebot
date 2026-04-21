#!/usr/bin/env python3
"""Daily cleanup: delete news older than 7 days from NewsStore sheet."""
import os
import time
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')
RETENTION_DAYS = 7


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def main():
    print(f"[{datetime.now()}] Starting news cleanup (retention={RETENTION_DAYS} days)...")
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='NewsStore!A2:G'
    ).execute()
    rows = result.get('values', [])
    print(f"Found {len(rows)} articles in NewsStore")

    if not rows:
        print("Nothing to clean up")
        return

    cutoff = datetime.now() - timedelta(days=RETENTION_DAYS)
    keep = []
    removed = 0

    for row in rows:
        if len(row) < 2:
            continue
        try:
            date = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
            if date >= cutoff:
                keep.append(row)
            else:
                removed += 1
        except ValueError:
            keep.append(row)

    if removed == 0:
        print("No old articles to remove")
        return

    print(f"Removing {removed} articles older than {cutoff.strftime('%Y-%m-%d')}")
    print(f"Keeping {len(keep)} articles")

    last_row = len(rows) + 1
    for attempt in range(5):
        try:
            sheets.clear(
                spreadsheetId=SPREADSHEET_ID,
                range=f'NewsStore!A2:G{last_row}'
            ).execute()
            break
        except Exception as e:
            if attempt < 4 and ('429' in str(e) or 'Timeout' in str(e)):
                time.sleep(30 * (attempt + 1))
            else:
                raise

    if keep:
        for attempt in range(5):
            try:
                sheets.update(
                    spreadsheetId=SPREADSHEET_ID,
                    range='NewsStore!A2:G',
                    valueInputOption='RAW',
                    body={'values': keep}
                ).execute()
                break
            except Exception as e:
                if attempt < 4 and ('429' in str(e) or 'Timeout' in str(e)):
                    time.sleep(30 * (attempt + 1))
                else:
                    raise

    print(f"[{datetime.now()}] Done! Removed {removed}, kept {len(keep)}")


if __name__ == '__main__':
    main()
