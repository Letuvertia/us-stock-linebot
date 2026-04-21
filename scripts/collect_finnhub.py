#!/usr/bin/env python3
"""Fetch Finnhub data for all tickers in StockUniverse and update Google Sheet."""
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
FINNHUB_TOKEN = os.environ.get('FINNHUB_API_KEY', 'd7j6kahr01qp3g1s085gd7j6kahr01qp3g1s0860')
FINNHUB_BASE = 'https://finnhub.io/api/v1'
RATE_LIMIT_DELAY = 1.1  # 60 calls/min = 1 call/sec + buffer

HEADER = [
    'Ticker', 'Exchange', 'Name',
    'Finnhub_Current_Price', 'Finnhub_Change_Pct', 'Finnhub_Open', 'Finnhub_High', 'Finnhub_Low', 'Finnhub_Prev_Close',
    'Finnhub_52W_High', 'Finnhub_52W_Low', 'Finnhub_Dist_From_High_Pct',
    'Finnhub_StrongBuy', 'Finnhub_Buy', 'Finnhub_Hold', 'Finnhub_Sell', 'Finnhub_StrongSell', 'Finnhub_Rating_Score',
    'Finnhub_PE_TTM', 'Finnhub_Forward_PE', 'Finnhub_PEG', 'Finnhub_EPS_TTM', 'Finnhub_EPS_Growth_Pct',
    'Finnhub_Beta', 'Finnhub_Market_Cap_M', 'Finnhub_Dividend_Yield',
    'Finnhub_Operating_Margin', 'Finnhub_Net_Margin', 'Finnhub_ROE',
    'Finnhub_Industry', 'Finnhub_Updated_At',
]


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def sheets_update_with_retry(sheets, range_, values, value_input='USER_ENTERED', retries=3):
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


def finnhub_get(endpoint: str) -> dict | list | None:
    url = f"{FINNHUB_BASE}{endpoint}{'&' if '?' in endpoint else '?'}token={FINNHUB_TOKEN}"
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        if e.code == 429:
            print(f"  Rate limited, sleeping 60s...")
            time.sleep(60)
            return finnhub_get(endpoint)
        print(f"  HTTP {e.code} for {endpoint}")
    except Exception as e:
        print(f"  Error: {e}")
    return None


def calc_rating_score(rec: dict) -> float:
    sb = rec.get('strongBuy', 0)
    b = rec.get('buy', 0)
    h = rec.get('hold', 0)
    s = rec.get('sell', 0)
    ss = rec.get('strongSell', 0)
    total = sb + b + h + s + ss
    if total == 0:
        return 0
    return round((sb * 5 + b * 4 + h * 3 + s * 2 + ss * 1) / total, 2)


def fetch_ticker_data(ticker: str, exchange: str, name: str) -> list:
    """Fetch all Finnhub data for a single ticker and return a row."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 1. Quote
    quote = finnhub_get(f'/quote?symbol={ticker}')
    time.sleep(RATE_LIMIT_DELAY)

    # 2. Recommendation
    recs = finnhub_get(f'/stock/recommendation?symbol={ticker}')
    time.sleep(RATE_LIMIT_DELAY)

    # 3. Metrics
    metric_data = finnhub_get(f'/stock/metric?symbol={ticker}&metric=all')
    time.sleep(RATE_LIMIT_DELAY)

    q = quote or {}
    current_price = q.get('c', '')
    change_pct = q.get('dp', '')
    open_price = q.get('o', '')
    high = q.get('h', '')
    low = q.get('l', '')
    prev_close = q.get('pc', '')

    rec = recs[0] if recs and len(recs) > 0 else {}
    strong_buy = rec.get('strongBuy', '')
    buy = rec.get('buy', '')
    hold = rec.get('hold', '')
    sell = rec.get('sell', '')
    strong_sell = rec.get('strongSell', '')
    rating_score = calc_rating_score(rec) if rec else ''

    m = (metric_data or {}).get('metric', {})
    w52_high = m.get('52WeekHigh', '')
    w52_low = m.get('52WeekLow', '')

    dist_from_high = ''
    if current_price and w52_high and w52_high > 0:
        dist_from_high = round(((w52_high - current_price) / current_price) * 100, 2)

    pe_ttm = m.get('peTTM', '')
    forward_pe = m.get('forwardPE', '')
    if forward_pe:
        forward_pe = round(forward_pe, 2)
    peg = m.get('pegTTM', '')
    if peg:
        peg = round(peg, 2)
    eps_ttm = m.get('epsTTM', '')
    eps_growth = m.get('epsGrowthTTMYoy', '')
    beta = m.get('beta', '')
    if beta:
        beta = round(beta, 2)
    market_cap = m.get('marketCapitalization', '')
    if market_cap:
        market_cap = round(market_cap, 2)
    div_yield = m.get('dividendYieldIndicatedAnnual', '')
    if div_yield:
        div_yield = round(div_yield, 2)
    op_margin = m.get('operatingMarginTTM', '')
    net_margin = m.get('netProfitMarginTTM', '')
    roe = m.get('roeTTM', '')

    industry = ''
    # Profile is optional — skip to save API calls; use existing Name/Exchange

    return [
        ticker, exchange, name,
        current_price, change_pct, open_price, high, low, prev_close,
        w52_high, w52_low, dist_from_high,
        strong_buy, buy, hold, sell, strong_sell, rating_score,
        pe_ttm, forward_pe, peg, eps_ttm, eps_growth,
        beta, market_cap, div_yield,
        op_margin, net_margin, roe,
        industry, now,
    ]


def main():
    print(f"[{datetime.now()}] Starting Finnhub data collection...")
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    # Read existing tickers (columns A:C)
    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:C'
    ).execute()
    rows = result.get('values', [])
    print(f"Found {len(rows)} tickers in StockUniverse")

    updated_count = 0
    for i, row in enumerate(rows):
        ticker = row[0] if len(row) > 0 else ''
        exchange = row[1] if len(row) > 1 else ''
        name = row[2] if len(row) > 2 else ''

        if not ticker:
            continue

        print(f"[{i+1}/{len(rows)}] Fetching {ticker}...", end=' ')
        try:
            data = fetch_ticker_data(ticker, exchange, name)
            price = data[3]
            print(f"${price}" if price else "no data")
        except Exception as e:
            print(f"ERROR: {e}")
            data = [ticker, exchange, name] + [''] * 28

        row_num = i + 2
        sheets_update_with_retry(sheets, f'StockUniverse!A{row_num}:AE{row_num}', [data])
        updated_count += 1

    print(f"\nUpdated {updated_count} rows in StockUniverse")
    print(f"[{datetime.now()}] Done!")


if __name__ == '__main__':
    main()
