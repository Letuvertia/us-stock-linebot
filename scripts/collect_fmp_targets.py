#!/usr/bin/env python3
"""Daily FMP price target collector. Updates Target columns in StockUniverse sheet.
Free tier: 250 calls/day. 154 tickers = 154 calls. Run once daily.
"""
import os
import time
import json
import warnings
import urllib.request
import urllib.error
from datetime import datetime

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')
FMP_API_KEY = os.environ.get('FMP_API_KEY', 'qXEMjVQ4E4Xhha7RpQOo2ZR50WtozY0k')
FMP_BASE = 'https://financialmodelingprep.com/stable'

# Target columns: AF=Target_High, AG=Target_Low, AH=Target_Consensus, AI=Target_Median, AJ=Upside_Pct, AK=FMP_Updated_At
TARGET_COL_START = 'AF'
TARGET_COL_END = 'AK'


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def sheets_update_with_retry(sheets, range_, values, value_input='RAW', retries=3):
    for attempt in range(retries):
        try:
            sheets.update(
                spreadsheetId=SPREADSHEET_ID, range=range_,
                valueInputOption=value_input, body={'values': values}
            ).execute()
            return
        except Exception as e:
            if '429' in str(e) and attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Sheets quota hit, waiting {wait}s...")
                time.sleep(wait)
            else:
                raise


def fmp_get(endpoint: str) -> list | None:
    url = f"{FMP_BASE}/{endpoint}{'&' if '?' in endpoint else '?'}apikey={FMP_API_KEY}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  Rate limited! Daily quota may be exhausted.")
            return None
        print(f"  HTTP {e.code}")
    except Exception as e:
        print(f"  Error: {e}")
    return None


def main():
    # FMP_BATCH: "even" = first 250, "odd" = last 250, unset = all
    # GitHub Actions passes this based on day-of-year
    batch = os.environ.get('FMP_BATCH', '')

    print(f"[{datetime.now()}] Starting FMP price target collection (batch={batch or 'all'})...")
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    # Read tickers and current prices (columns A and D)
    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:D'
    ).execute()
    rows = result.get('values', [])
    total = len(rows)
    print(f"Found {total} tickers")

    if batch == 'first':
        print(f"Batch: first 250 tickers (1-250)")
    elif batch == 'second':
        print(f"Batch: last {total - 250} tickers (251-{total})")

    updated_count = 0
    for i, row in enumerate(rows):
        if batch == 'first' and i >= 250:
            break
        if batch == 'second' and i < 250:
            continue
        ticker = row[0] if len(row) > 0 else ''
        current_price = float(row[3]) if len(row) > 3 and row[3] else 0
        row_num = i + 2  # row 1 is header

        if not ticker:
            continue

        print(f"[{i+1}/{len(rows)}] {ticker}...", end=' ')

        data = fmp_get(f'price-target-consensus?symbol={ticker}')
        time.sleep(0.5)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if not data or len(data) == 0:
            print("no target data")
            # Write empty values but still update timestamp
            row_data = ['', '', '', '', '', now]
        else:
            d = data[0]
            target_high = d.get('targetHigh', '')
            target_low = d.get('targetLow', '')
            target_consensus = d.get('targetConsensus', '')
            target_median = d.get('targetMedian', '')

            upside = ''
            if target_consensus and current_price > 0:
                upside = round(((target_consensus - current_price) / current_price) * 100, 2)

            print(f"consensus=${target_consensus} upside={upside}%")
            row_data = [target_high, target_low, target_consensus, target_median, upside, now]

        sheets_update_with_retry(sheets, f'StockUniverse!{TARGET_COL_START}{row_num}:{TARGET_COL_END}{row_num}', [row_data])
        updated_count += 1

    print(f"\nUpdated {updated_count} rows with target prices")

    print(f"[{datetime.now()}] Done!")


if __name__ == '__main__':
    main()
