#!/usr/bin/env python3
"""One-time backfill: populate missing Exchange and Name columns using Finnhub profile2."""
import os
import time
import json
import urllib.request
import urllib.error

from google.oauth2 import service_account
from googleapiclient.discovery import build

CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')
FINNHUB_TOKEN = os.environ.get('FINNHUB_API_KEY', 'd7j6kahr01qp3g1s085gd7j6kahr01qp3g1s0860')


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def finnhub_profile(ticker: str) -> dict | None:
    url = f"https://finnhub.io/api/v1/stock/profile2?symbol={ticker}&token={FINNHUB_TOKEN}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  Rate limited, sleeping 60s...")
            time.sleep(60)
            return finnhub_profile(ticker)
        print(f"  HTTP {e.code}")
    except Exception as e:
        print(f"  Error: {e}")
    return None


def main():
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:C'
    ).execute()
    rows = result.get('values', [])
    print(f"Found {len(rows)} tickers")

    updated = 0
    for i, row in enumerate(rows):
        ticker = row[0] if len(row) > 0 else ''
        exchange = row[1] if len(row) > 1 else ''
        name = row[2] if len(row) > 2 else ''

        if not ticker or (exchange and name):
            continue

        print(f"[{i+1}/{len(rows)}] {ticker}...", end=' ')
        profile = finnhub_profile(ticker)
        time.sleep(1.1)

        if not profile or not profile.get('name'):
            print("no profile")
            continue

        new_exchange = profile.get('exchange', exchange)
        new_name = profile.get('name', name)
        row_num = i + 2

        print(f"{new_exchange} / {new_name}")

        for attempt in range(5):
            try:
                sheets.update(
                    spreadsheetId=SPREADSHEET_ID,
                    range=f'StockUniverse!B{row_num}:C{row_num}',
                    valueInputOption='RAW',
                    body={'values': [[new_exchange, new_name]]}
                ).execute()
                updated += 1
                break
            except Exception as e:
                if attempt < 4 and ('429' in str(e) or 'Timeout' in str(e)):
                    time.sleep(30 * (attempt + 1))
                else:
                    raise

    print(f"\nBackfilled {updated} tickers with Exchange/Name")


if __name__ == '__main__':
    main()
