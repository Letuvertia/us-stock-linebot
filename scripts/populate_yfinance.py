#!/usr/bin/env python3
"""Populate stock sheets with Yahoo Finance historical data. Runs in parallel with migration."""
import json
import time
import sys
import os

import yfinance as yf

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import _is_retryable

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

STATE_FILE = '/tmp/migration_state.json'
OAUTH_TOKEN_FILE = '/tmp/oauth_token.json'
POPULATE_STATE_FILE = '/tmp/populate_state.json'


def get_oauth_sheets():
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


def load_populate_state():
    if os.path.exists(POPULATE_STATE_FILE):
        with open(POPULATE_STATE_FILE) as f:
            return json.load(f)
    return {'populated': []}


def save_populate_state(state):
    with open(POPULATE_STATE_FILE, 'w') as f:
        json.dump(state, f)


def fetch_yfinance(ticker: str) -> list[list]:
    t = yf.Ticker(ticker)
    df = t.history(period='max')
    if df.empty:
        return []
    rows = []
    for date, row in df.iterrows():
        rows.append([
            date.strftime('%Y-%m-%d'),
            round(row['Close'], 4),
            round(row['High'], 4),
            round(row['Low'], 4),
            round(row['Open'], 4),
            int(row['Volume']),
            '',  # Kaggle_Adj_Close — leave empty, yfinance already adjusts
        ])
    return rows


def expand_sheet_rows(sheets, spreadsheet_id: str, needed_rows: int):
    meta = sheets.spreadsheets().get(
        spreadsheetId=spreadsheet_id, fields='sheets.properties'
    ).execute()
    sheet_props = meta['sheets'][0]['properties']
    sheet_id = sheet_props['sheetId']
    current_rows = sheet_props['gridProperties']['rowCount']
    if current_rows >= needed_rows:
        return
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={'requests': [{'updateSheetProperties': {
            'properties': {'sheetId': sheet_id, 'gridProperties': {'rowCount': needed_rows}},
            'fields': 'gridProperties.rowCount',
        }}]},
    ).execute()


def write_to_sheet(sheets, spreadsheet_id: str, data: list[list]):
    expand_sheet_rows(sheets, spreadsheet_id, len(data) + 2)
    time.sleep(0.5)

    for i in range(0, len(data), 1000):
        chunk = data[i:i + 1000]
        start_row = i + 2
        for attempt in range(5):
            try:
                sheets.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
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
        time.sleep(0.5)


def main():
    sheets = get_oauth_sheets()
    pop_state = load_populate_state()
    populated = set(pop_state['populated'])

    pass_num = 0
    while True:
        pass_num += 1
        with open(STATE_FILE) as f:
            migration = json.load(f)

        created = migration.get('created_stock_sheets', {})
        todo = {t: sid for t, sid in created.items() if t not in populated}

        if not todo:
            if migration.get('phase') in ('populate_stock_data', 'complete'):
                print(f"All {len(populated)} tickers populated. Done!")
                break
            print(f"Pass {pass_num}: no new sheets to populate yet ({len(populated)} done, waiting for more sheets)...")
            time.sleep(30)
            continue

        print(f"Pass {pass_num}: {len(todo)} sheets to populate ({len(populated)} already done)")

        for ticker, sid in sorted(todo.items()):
            try:
                data = fetch_yfinance(ticker)
                if not data:
                    print(f"  {ticker}: no data from yfinance, skipping")
                    populated.add(ticker)
                    pop_state['populated'] = list(populated)
                    save_populate_state(pop_state)
                    continue

                write_to_sheet(sheets, sid, data)
                populated.add(ticker)
                pop_state['populated'] = list(populated)
                save_populate_state(pop_state)
                print(f"  {ticker}: {len(data)} rows written ✓")
            except Exception as e:
                print(f"  {ticker}: FAILED - {e}")

            time.sleep(1.5)

        # Check if migration is still creating sheets
        with open(STATE_FILE) as f:
            migration = json.load(f)
        if migration.get('phase') in ('populate_stock_data', 'complete'):
            remaining = {t for t in migration['created_stock_sheets'] if t not in populated}
            if not remaining:
                print(f"\nAll {len(populated)} tickers populated. Done!")
                break

    print(f"\nTotal populated: {len(populated)}")


if __name__ == '__main__':
    main()
