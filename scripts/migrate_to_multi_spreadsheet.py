#!/usr/bin/env python3
"""Migrate from single spreadsheet to multi-spreadsheet folder structure."""
import os
import sys
import csv
import json
import time
from datetime import datetime

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, US_STOCK_SPREADSHEET_ID, ROOT_FOLDER_ID,
    get_sheets_service, get_drive_service, _is_retryable,
)

USER_EMAIL = '1026oscar1@gmail.com'
SERVICE_ACCOUNT_EMAIL = 'claude-sheets-access@juns-stock-agent.iam.gserviceaccount.com'
STATE_FILE = os.environ.get('MIGRATION_STATE_FILE', '/tmp/migration_state.json')
OAUTH_TOKEN_FILE = '/tmp/oauth_token.json'


def get_oauth_sheets_service():
    with open(OAUTH_TOKEN_FILE) as f:
        info = json.load(f)
    creds = Credentials(
        token=info['token'],
        refresh_token=info['refresh_token'],
        token_uri=info['token_uri'],
        client_id=info['client_id'],
        client_secret=info['client_secret'],
        scopes=info['scopes'],
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def get_oauth_drive_service():
    with open(OAUTH_TOKEN_FILE) as f:
        info = json.load(f)
    creds = Credentials(
        token=info['token'],
        refresh_token=info['refresh_token'],
        token_uri=info['token_uri'],
        client_id=info['client_id'],
        client_secret=info['client_secret'],
        scopes=info['scopes'],
    )
    return build('drive', 'v3', credentials=creds, cache_discovery=False)

COMPANIES_CSV = '/mnt/c/Users/1026o/Desktop/us-stock-linebot/sp500/sp500_companies.csv'
STOCKS_CSV = '/mnt/c/Users/1026o/Desktop/us-stock-linebot/sp500/sp500_stocks.csv'

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

NEWS_HEADERS = ['ID', 'Date', 'TickerTags', 'Title', 'Content', 'URL', 'Processed_At']

# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        print(f"Resuming from phase: {state['phase']}")
        print(f"  Stock sheets created: {len(state['created_stock_sheets'])}/500")
        print(f"  Tickers populated: {len(state['populated_tickers'])}")
        return state
    return {
        'phase': 'create_folders',
        'created_folders': {},
        'created_spreadsheets': {},
        'created_stock_sheets': {},
        'populated_tickers': [],
    }


def save_state(state: dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def api_retry(fn, *args, retries=5, **kwargs):
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs).execute()
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"    Retryable error ({type(e).__name__}), waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def create_folder(drive, name: str, parent_id: str) -> str:
    q = f"name='{name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    existing = drive.files().list(q=q, fields='files(id)').execute().get('files', [])
    if existing:
        print(f"  Folder '{name}' already exists")
        return existing[0]['id']

    meta = {'name': name, 'mimeType': 'application/vnd.google-apps.folder', 'parents': [parent_id]}
    folder = drive.files().create(body=meta, fields='id').execute()
    print(f"  Created folder '{name}' → {folder['id']}")
    time.sleep(1)
    return folder['id']


def share_with_service_account(drive, file_id: str):
    api_retry(
        drive.permissions().create,
        fileId=file_id,
        body={'type': 'user', 'role': 'writer', 'emailAddress': SERVICE_ACCOUNT_EMAIL},
        fields='id',
    )
    time.sleep(1)


def create_spreadsheet(sheets_svc, drive, name: str, parent_id: str,
                        tab_configs: list[dict]) -> str:
    """Create spreadsheet with tabs, headers, move to folder, share.

    tab_configs: [{'name': 'Sheet1', 'headers': [...], 'data': [[...], ...]}, ...]
    """
    body = {
        'properties': {'title': name},
        'sheets': [{'properties': {'title': tab_configs[0]['name']}}],
    }
    result = api_retry(sheets_svc.spreadsheets().create, body=body)
    sid = result['spreadsheetId']
    time.sleep(2)

    # Write first tab headers
    api_retry(
        sheets_svc.spreadsheets().values().update,
        spreadsheetId=sid, range=f"{tab_configs[0]['name']}!A1",
        valueInputOption='RAW', body={'values': [tab_configs[0]['headers']]},
    )
    time.sleep(1)

    # Write first tab data if any
    if tab_configs[0].get('data'):
        api_retry(
            sheets_svc.spreadsheets().values().update,
            spreadsheetId=sid,
            range=f"{tab_configs[0]['name']}!A2",
            valueInputOption='RAW', body={'values': tab_configs[0]['data']},
        )
        time.sleep(1)

    # Additional tabs
    for tc in tab_configs[1:]:
        api_retry(
            sheets_svc.spreadsheets().batchUpdate,
            spreadsheetId=sid,
            body={'requests': [{'addSheet': {'properties': {'title': tc['name']}}}]},
        )
        time.sleep(1)
        api_retry(
            sheets_svc.spreadsheets().values().update,
            spreadsheetId=sid, range=f"{tc['name']}!A1",
            valueInputOption='RAW', body={'values': [tc['headers']]},
        )
        time.sleep(1)
        if tc.get('data'):
            api_retry(
                sheets_svc.spreadsheets().values().update,
                spreadsheetId=sid, range=f"{tc['name']}!A2",
                valueInputOption='RAW', body={'values': tc['data']},
            )
            time.sleep(1)

    # Move to folder
    file_info = drive.files().get(fileId=sid, fields='parents').execute()
    prev_parents = ','.join(file_info.get('parents', []))
    drive.files().update(
        fileId=sid, addParents=parent_id, removeParents=prev_parents, fields='id',
    ).execute()
    time.sleep(1)

    # Share with service account so daily workflows can write
    share_with_service_account(drive, sid)
    return sid

# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_companies() -> dict:
    companies = {}
    with open(COMPANIES_CSV, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            companies[row['Symbol']] = {
                'exchange': row.get('Exchange', ''),
                'name': row.get('Longname') or row.get('Shortname', ''),
                'sector': row.get('Sector', ''),
                'industry': row.get('Industry', ''),
            }
    return companies


def load_existing_keywords(sheets_svc) -> dict:
    result = sheets_svc.spreadsheets().values().get(
        spreadsheetId=US_STOCK_SPREADSHEET_ID, range='StockUniverse!A2:AX',
    ).execute()
    kw = {}
    for row in result.get('values', []):
        if not row:
            continue
        ticker = row[0]
        keywords = row[49] if len(row) > 49 else ''
        kw[ticker] = keywords
    return kw


def load_stock_tickers(sheets_svc) -> list[dict]:
    result = sheets_svc.spreadsheets().values().get(
        spreadsheetId=US_STOCK_SPREADSHEET_ID, range='StockUniverse!A2:C',
    ).execute()
    tickers = []
    for row in result.get('values', []):
        if not row:
            continue
        tickers.append({
            'ticker': row[0],
            'exchange': row[1] if len(row) > 1 else '',
            'name': row[2] if len(row) > 2 else '',
        })
    return tickers


def load_kaggle_data() -> dict:
    print("Loading Kaggle CSV (this takes a moment for 92MB)...")
    ticker_rows = {}
    with open(STOCKS_CSV, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if not row['Close']:
                continue
            ticker_rows.setdefault(row['Symbol'], []).append(row)
    total = sum(len(v) for v in ticker_rows.values())
    print(f"  Loaded {total:,} data rows for {len(ticker_rows)} tickers")
    return ticker_rows

# ---------------------------------------------------------------------------
# Phase implementations
# ---------------------------------------------------------------------------

def phase1_create_folders(drive, state):
    print("\n[PHASE 1/5] Creating subfolders...")
    state['created_folders']['stocks'] = create_folder(drive, 'stocks', ROOT_FOLDER_ID)
    state['created_folders']['news'] = create_folder(drive, 'news', ROOT_FOLDER_ID)
    state['phase'] = 'create_root_sheets'
    save_state(state)


def phase2_create_root_sheets(oauth_sheets, drive, sa_sheets, state):
    print("\n[PHASE 2/5] Creating root-level spreadsheets...")

    # gas-logs
    if 'gas-logs' not in state['created_spreadsheets']:
        sid = create_spreadsheet(oauth_sheets, drive, 'gas-logs', ROOT_FOLDER_ID, [
            {'name': 'Logs', 'headers': ['Timestamp', 'Level', 'Function', 'Message']},
        ])
        state['created_spreadsheets']['gas-logs'] = sid
        print(f"  Created gas-logs → {sid}")
        save_state(state)

    # user-config
    if 'user-config' not in state['created_spreadsheets']:
        companies = load_companies()
        keywords = load_existing_keywords(sa_sheets)
        tickers = load_stock_tickers(sa_sheets)

        kw_data = []
        for t in tickers:
            ticker = t['ticker']
            comp = companies.get(ticker, {})
            kw_data.append([
                ticker,
                t['exchange'],
                t['name'],
                keywords.get(ticker, ''),
                comp.get('sector', ''),
                comp.get('industry', ''),
            ])

        sid = create_spreadsheet(oauth_sheets, drive, 'user-config', ROOT_FOLDER_ID, [
            {
                'name': 'News Keywords',
                'headers': ['Ticker', 'Exchange', 'Name', 'News Keywords',
                            'Industry Category', 'Industry SubCategory'],
                'data': kw_data,
            },
            {
                'name': 'Users',
                'headers': ['User Names', 'Watchlist', 'Holding'],
            },
        ])
        state['created_spreadsheets']['user-config'] = sid
        print(f"  Created user-config → {sid}")
        save_state(state)

    state['phase'] = 'create_news_sheets'
    save_state(state)


def phase3_create_news_sheets(oauth_sheets, drive, state):
    print("\n[PHASE 3/5] Creating news spreadsheets...")
    news_folder = state['created_folders']['news']

    for name in ['CNBC News', 'Reuters News']:
        if name not in state['created_spreadsheets']:
            sid = create_spreadsheet(oauth_sheets, drive, name, news_folder, [
                {'name': 'Sheet1', 'headers': NEWS_HEADERS},
            ])
            state['created_spreadsheets'][name] = sid
            print(f"  Created {name} → {sid}")
            save_state(state)

    state['phase'] = 'create_stock_sheets'
    save_state(state)


def phase4_create_stock_sheets(oauth_sheets, drive, sa_sheets, state):
    print("\n[PHASE 4/5] Creating 500 stock spreadsheets...")
    stocks_folder = state['created_folders']['stocks']
    tickers = load_stock_tickers(sa_sheets)
    total = len(tickers)

    for idx, t in enumerate(tickers, 1):
        ticker = t['ticker']
        if ticker in state['created_stock_sheets']:
            continue

        sheet_name = f"{ticker} - {t['name']}"
        try:
            sid = create_spreadsheet(oauth_sheets, drive, sheet_name, stocks_folder, [
                {'name': 'Sheet1', 'headers': STOCK_HEADERS},
            ])
            state['created_stock_sheets'][ticker] = sid
            save_state(state)
            print(f"  [{idx}/{total}] {sheet_name} ✓")
        except Exception as e:
            print(f"  [{idx}/{total}] {sheet_name} FAILED: {e}")

        if idx % 50 == 0:
            print(f"  --- checkpoint: {idx}/{total} created ---")

    state['phase'] = 'populate_stock_data'
    save_state(state)


def phase5_populate_stock_data(oauth_sheets, state):
    print("\n[PHASE 5/5] Populating Kaggle historical data...")
    kaggle = load_kaggle_data()

    tickers_to_populate = [
        t for t in kaggle if t in state['created_stock_sheets']
        and t not in state['populated_tickers']
    ]
    total = len(tickers_to_populate)
    print(f"  {total} tickers to populate")

    for idx, ticker in enumerate(tickers_to_populate, 1):
        sid = state['created_stock_sheets'][ticker]
        rows_raw = kaggle[ticker]

        data = []
        for r in rows_raw:
            data.append([
                r['Date'], r['Close'], r['High'], r['Low'],
                r['Open'], r['Volume'], r['Adj Close'],
            ])

        # Write in 1000-row batches
        for i in range(0, len(data), 1000):
            chunk = data[i:i + 1000]
            start_row = i + 2
            for attempt in range(5):
                try:
                    oauth_sheets.spreadsheets().values().update(
                        spreadsheetId=sid,
                        range=f"Sheet1!A{start_row}",
                        valueInputOption='RAW',
                        body={'values': chunk},
                    ).execute()
                    break
                except Exception as e:
                    if attempt < 4 and _is_retryable(e):
                        wait = 30 * (attempt + 1)
                        print(f"    Retry ({type(e).__name__}), waiting {wait}s...")
                        time.sleep(wait)
                    else:
                        raise
            time.sleep(1)

        state['populated_tickers'].append(ticker)
        save_state(state)
        print(f"  [{idx}/{total}] {ticker}: {len(data)} rows ✓")

    state['phase'] = 'complete'
    save_state(state)

# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def verify(drive, sheets_svc, state):
    print("\n=== VERIFICATION ===")

    # Count stock sheets
    stocks_folder = state['created_folders']['stocks']
    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=f"'{stocks_folder}' in parents and trashed=false",
            fields='nextPageToken, files(id, name)',
            pageToken=page_token,
            pageSize=100,
        ).execute()
        files.extend(resp.get('files', []))
        page_token = resp.get('nextPageToken')
        if not page_token:
            break
    print(f"Stock sheets in /stocks: {len(files)}/500 {'✓' if len(files) == 500 else 'INCOMPLETE'}")

    # Count news sheets
    news_folder = state['created_folders']['news']
    news_files = drive.files().list(
        q=f"'{news_folder}' in parents and trashed=false",
        fields='files(id, name)',
    ).execute().get('files', [])
    print(f"News sheets in /news: {len(news_files)}/2 {'✓' if len(news_files) == 2 else 'INCOMPLETE'}")

    # Sample stock data
    for ticker in ['MSFT', 'NVDA', 'AMZN']:
        if ticker in state['created_stock_sheets']:
            sid = state['created_stock_sheets'][ticker]
            result = sheets_svc.spreadsheets().values().get(
                spreadsheetId=sid, range='Sheet1!A:A',
            ).execute()
            rows = len(result.get('values', [])) - 1
            print(f"  {ticker}: {rows} data rows")

    # user-config
    if 'user-config' in state['created_spreadsheets']:
        sid = state['created_spreadsheets']['user-config']
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=sid, range="'News Keywords'!A:A",
        ).execute()
        rows = len(result.get('values', [])) - 1
        print(f"user-config News Keywords: {rows}/500 {'✓' if rows == 500 else 'INCOMPLETE'}")

    print(f"Populated tickers: {len(state['populated_tickers'])}")
    print("=== DONE ===")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    start = datetime.now(UTC8)
    print(f"[{start}] === Multi-Spreadsheet Migration ===")

    state = load_state()
    # Service account for reading existing data
    sheets_svc = get_sheets_service()
    # OAuth user credentials for creating files (service account has 0 storage quota)
    oauth_sheets = get_oauth_sheets_service()
    oauth_drive = get_oauth_drive_service()
    drive = oauth_drive

    if state['phase'] == 'create_folders':
        phase1_create_folders(drive, state)

    if state['phase'] == 'create_root_sheets':
        phase2_create_root_sheets(oauth_sheets, drive, sheets_svc, state)

    if state['phase'] == 'create_news_sheets':
        phase3_create_news_sheets(oauth_sheets, drive, state)

    if state['phase'] == 'create_stock_sheets':
        phase4_create_stock_sheets(oauth_sheets, drive, sheets_svc, state)

    if state['phase'] == 'populate_stock_data':
        phase5_populate_stock_data(oauth_sheets, state)

    verify(drive, oauth_sheets, state)

    elapsed = datetime.now(UTC8) - start
    print(f"\nTotal time: {elapsed}")
    print(f"State file: {STATE_FILE}")


if __name__ == '__main__':
    main()
