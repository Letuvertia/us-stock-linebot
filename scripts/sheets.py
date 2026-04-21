#!/usr/bin/env python3
"""Helper script to read/write Google Sheets via service account."""
import sys
import warnings
warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

CREDS_FILE = '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json'
SPREADSHEET_ID = '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU'

def get_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)

def read_sheet(sheet_name, range_suffix='', limit=None):
    service = get_service()
    r = f"{sheet_name}!{range_suffix}" if range_suffix else sheet_name
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=r
    ).execute()
    rows = result.get('values', [])
    if limit:
        rows = rows[-limit:]
    return rows

def logs(n=20):
    rows = read_sheet('SystemLogs', 'A:D')
    for row in rows[-n:]:
        print('  |  '.join(str(c) for c in row))

if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'logs'
    if cmd == 'logs':
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        logs(n)
    elif cmd == 'read':
        sheet = sys.argv[2] if len(sys.argv) > 2 else 'SystemLogs'
        rows = read_sheet(sheet)
        for row in rows:
            print('  |  '.join(str(c) for c in row))
    else:
        print(f"Usage: {sys.argv[0]} [logs [N] | read <SheetName>]")
