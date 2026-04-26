#!/usr/bin/env python3
"""Clean up ticker list: delete extras, create missing sheets, update user-config and StockUniverse."""
import csv
import json
import time
import sys
import os

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    SPREADSHEET_ID, ROOT_FOLDER_ID,
    get_sheets_service, _is_retryable,
)

OAUTH_TOKEN_FILE = '/tmp/oauth_token.json'
STATE_FILE = '/tmp/migration_state.json'
COMPANIES_CSV = '/mnt/c/Users/1026o/Desktop/us-stock-linebot/sp500/sp500_companies.csv'
USER_CONFIG_SPREADSHEET_ID = '1rIVv2lZDrUT7bCO8iXzl5g5J_-BKA7RjusT64akZD0k'
SERVICE_ACCOUNT_EMAIL = 'claude-sheets-access@juns-stock-agent.iam.gserviceaccount.com'

EXCLUDE = {'GOOG'}
RENAME = {'BRK-B': 'BRK.B', 'BF-B': 'BF.B', 'FI': 'FISV', 'MMC': 'MRSH'}

STOCK_HEADERS = [
    'Date', 'Close', 'High', 'Low', 'Open', 'Volume', 'Kaggle_Adj_Close',
    'Finnhub_52W_High', 'Finnhub_52W_Low', 'Finnhub_Dist_From_High_Pct',
    'Finnhub_StrongBuy', 'Finnhub_Buy', 'Finnhub_Hold',
    'Finnhub_Sell', 'Finnhub_StrongSell', 'Finnhub_Rating_Score',
    'Finnhub_PE_TTM', 'Finnhub_Forward_PE', 'Finnhub_PEG',
    'Finnhub_EPS_TTM', 'Finnhub_EPS_Growth_Pct',
    'Finnhub_Beta', 'Finnhub_Market_Cap_M', 'Finnhub_Dividend_Yield',
    'Finnhub_Operating_Margin', 'Finnhub_Net_Margin', 'Finnhub_ROE',
]


def get_oauth_creds():
    with open(OAUTH_TOKEN_FILE) as f:
        info = json.load(f)
    return Credentials(
        token=info['token'],
        refresh_token=info['refresh_token'],
        token_uri=info['token_uri'],
        client_id=info['client_id'],
        client_secret=info['client_secret'],
        scopes=info['scopes'],
    )


def build_target_list():
    with open(COMPANIES_CSV, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    target = {}
    for r in rows:
        sym = r['Symbol']
        if sym in EXCLUDE:
            continue
        final_sym = RENAME.get(sym, sym)
        target[final_sym] = {
            'name': r.get('Longname') or r.get('Shortname', ''),
            'exchange': r.get('Exchange', ''),
            'sector': r.get('Sector', ''),
            'industry': r.get('Industry', ''),
            'original_sym': sym,
        }
    return target


def api_retry(fn, *args, retries=5, **kwargs):
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs).execute()
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"    Retry ({type(e).__name__}), waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def main():
    target = build_target_list()
    print(f"Target ticker list: {len(target)} tickers")

    with open(STATE_FILE) as f:
        state = json.load(f)
    created = state['created_stock_sheets']
    stocks_folder = state['created_folders']['stocks']

    to_delete = sorted(set(created.keys()) - set(target.keys()))
    to_create = sorted(set(target.keys()) - set(created.keys()))
    print(f"To delete: {len(to_delete)}")
    print(f"To create: {len(to_create)}")

    creds = get_oauth_creds()
    oauth_drive = build('drive', 'v3', credentials=creds, cache_discovery=False)
    oauth_sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)

    # 1. Delete extra sheets
    print("\n=== Deleting extra sheets ===")
    for ticker in to_delete:
        sid = created[ticker]
        try:
            oauth_drive.files().delete(fileId=sid).execute()
            del state['created_stock_sheets'][ticker]
            print(f"  Deleted {ticker}")
            time.sleep(0.5)
        except Exception as e:
            print(f"  Failed to delete {ticker}: {e}")
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

    # 2. Create missing sheets
    print(f"\n=== Creating {len(to_create)} missing sheets ===")
    for idx, ticker in enumerate(to_create, 1):
        info = target[ticker]
        sheet_name = f"{ticker} - {info['name']}"
        try:
            result = api_retry(oauth_sheets.spreadsheets().create,
                               body={'properties': {'title': sheet_name},
                                     'sheets': [{'properties': {'title': 'Sheet1'}}]})
            sid = result['spreadsheetId']
            time.sleep(1)

            api_retry(oauth_sheets.spreadsheets().values().update,
                      spreadsheetId=sid, range='Sheet1!A1',
                      valueInputOption='RAW', body={'values': [STOCK_HEADERS]})
            time.sleep(1)

            # Move to stocks folder
            file_info = oauth_drive.files().get(fileId=sid, fields='parents').execute()
            prev_parents = ','.join(file_info.get('parents', []))
            oauth_drive.files().update(fileId=sid, addParents=stocks_folder,
                                       removeParents=prev_parents, fields='id').execute()
            time.sleep(1)

            # Share with service account
            oauth_drive.permissions().create(
                fileId=sid,
                body={'type': 'user', 'role': 'writer', 'emailAddress': SERVICE_ACCOUNT_EMAIL},
                fields='id',
            ).execute()
            time.sleep(1)

            state['created_stock_sheets'][ticker] = sid
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, indent=2)
            print(f"  [{idx}/{len(to_create)}] {sheet_name} ✓")
        except Exception as e:
            print(f"  [{idx}/{len(to_create)}] {sheet_name} FAILED: {e}")

    # 3. Update user-config "News Keywords" tab
    print("\n=== Updating user-config News Keywords ===")
    sa_sheets = get_sheets_service()

    # Read existing keywords from StockUniverse
    result = sa_sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range='StockUniverse!A2:AX'
    ).execute()
    existing_kw = {}
    for row in result.get('values', []):
        if not row:
            continue
        t = row[0]
        kw = row[49] if len(row) > 49 else ''
        existing_kw[t] = kw

    # Build new News Keywords data sorted by ticker
    kw_data = []
    for ticker in sorted(target.keys()):
        info = target[ticker]
        kw_data.append([
            ticker,
            info['exchange'],
            info['name'],
            existing_kw.get(ticker, ''),
            info['sector'],
            info['industry'],
        ])

    # Clear and rewrite
    oauth_sheets.spreadsheets().values().clear(
        spreadsheetId=USER_CONFIG_SPREADSHEET_ID,
        range="'News Keywords'!A2:F",
    ).execute()
    time.sleep(1)

    oauth_sheets.spreadsheets().values().update(
        spreadsheetId=USER_CONFIG_SPREADSHEET_ID,
        range="'News Keywords'!A2",
        valueInputOption='RAW',
        body={'values': kw_data},
    ).execute()
    print(f"  Written {len(kw_data)} rows to News Keywords")

    # 4. Update StockUniverse
    print("\n=== Updating StockUniverse ===")
    # Read all existing data
    result = sa_sheets.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range='StockUniverse!A2:AX'
    ).execute()
    old_rows = result.get('values', [])
    old_data = {}
    for row in old_rows:
        if row:
            old_data[row[0]] = row

    # Build new rows preserving existing data where possible
    new_rows = []
    for ticker in sorted(target.keys()):
        info = target[ticker]
        if ticker in old_data:
            row = old_data[ticker]
            # Update name/exchange in case they differ
            while len(row) < 3:
                row.append('')
            row[1] = info['exchange']
            row[2] = info['name']
            new_rows.append(row)
        else:
            row = [ticker, info['exchange'], info['name']]
            new_rows.append(row)

    # Pad rows to same length
    max_len = max(len(r) for r in new_rows)
    for r in new_rows:
        while len(r) < max_len:
            r.append('')

    # Clear and rewrite
    sa_sheets.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range='StockUniverse!A2:AX'
    ).execute()
    time.sleep(1)

    sa_sheets.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range='StockUniverse!A2',
        valueInputOption='RAW',
        body={'values': new_rows},
    ).execute()
    print(f"  Written {len(new_rows)} rows to StockUniverse")

    # Summary
    print(f"\n=== DONE ===")
    print(f"Total sheets: {len(state['created_stock_sheets'])}")
    print(f"News Keywords rows: {len(kw_data)}")
    print(f"StockUniverse rows: {len(new_rows)}")


if __name__ == '__main__':
    main()
