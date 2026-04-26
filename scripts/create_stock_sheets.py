#!/usr/bin/env python3
"""Create 500 stock sheets in /stocks folder with 170-column Data Schema headers."""
import json
import os
import sys
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import US_STOCK_SPREADSHEET_ID, get_sheets_service, _is_retryable

OAUTH_TOKEN_FILE = '/tmp/oauth_token.json'
STATE_FILE = '/tmp/create_sheets_state.json'
STOCKS_FOLDER_ID = '1E6oTHrvqwVtxrjOgE2XgK78f_n76zRVc'


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


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {'created': {}}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def main():
    sa_sheets = get_sheets_service().spreadsheets().values()

    # 1. Read ticker list from StockUniverse
    result = sa_sheets.get(spreadsheetId=US_STOCK_SPREADSHEET_ID, range='StockUniverse!A2:C').execute()
    tickers = result.get('values', [])
    print(f"Loaded {len(tickers)} tickers from StockUniverse")

    # 2. Read column names from Data Schema
    result = sa_sheets.get(spreadsheetId=US_STOCK_SPREADSHEET_ID, range='Data Schema!A2:A200').execute()
    headers = [r[0] for r in result.get('values', []) if r]
    print(f"Header row: {len(headers)} columns (starts with {headers[0]})")

    # 3. Build OAuth services
    creds = get_oauth_creds()
    oauth_sheets = build('sheets', 'v4', credentials=creds, cache_discovery=False)
    oauth_drive = build('drive', 'v3', credentials=creds, cache_discovery=False)

    state = load_state()
    created = state['created']

    to_create = [(r[0], r[2] if len(r) > 2 else '') for r in tickers if r[0] not in created]
    print(f"Already created: {len(created)}, remaining: {len(to_create)}")

    for idx, (ticker, name) in enumerate(to_create, 1):
        sheet_title = f"{ticker} - {name}" if name else ticker
        print(f"[{idx}/{len(to_create)}] {sheet_title}...", end=' ', flush=True)

        try:
            # Step 1: Create spreadsheet with tab named "Daily"
            result = api_retry(
                oauth_sheets.spreadsheets().create,
                body={
                    'properties': {'title': sheet_title},
                    'sheets': [{'properties': {'title': 'Daily'}}],
                },
            )
            sid = result['spreadsheetId']
            time.sleep(0.3)

            # Step 2: Write header row
            api_retry(
                oauth_sheets.spreadsheets().values().update,
                spreadsheetId=sid,
                range='Daily!A1',
                valueInputOption='RAW',
                body={'values': [headers]},
            )
            time.sleep(0.3)

            # Step 3: Move to stocks folder
            file_info = oauth_drive.files().get(fileId=sid, fields='parents').execute()
            prev_parents = ','.join(file_info.get('parents', []))
            oauth_drive.files().update(
                fileId=sid,
                addParents=STOCKS_FOLDER_ID,
                removeParents=prev_parents,
                fields='id',
            ).execute()
            time.sleep(0.4)

            created[ticker] = sid
            save_state(state)
            print("done")
        except Exception as e:
            print(f"FAILED: {e}")

    print(f"\n=== Complete: {len(created)} sheets created ===")


if __name__ == '__main__':
    main()
