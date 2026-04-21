#!/usr/bin/env python3
"""Daily MarketWatch analyst estimates collector.
Scrapes target prices + EPS estimates from MarketWatch analyst estimates pages.
Writes to StockUniverse columns AL-AW (indices 37-48). Run once daily.
"""
import os
import time
import re
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
MW_BASE = 'https://www.marketwatch.com/investing/stock'

COL_START = 'AL'
COL_END = 'AW'
REQUEST_DELAY = float(os.environ.get('MW_REQUEST_DELAY', '5.0'))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.google.com/',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def fetch_page(ticker: str) -> str | None:
    url = f"{MW_BASE}/{ticker.lower()}/analystestimates"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                return resp.read().decode()
    except urllib.error.HTTPError as e:
        if e.code == 403:
            print(f"blocked")
        elif e.code == 404:
            print(f"no page")
        else:
            print(f"HTTP {e.code}")
    except Exception as e:
        print(f"error: {e}")
    return None


def _parse_cells(row_html: str) -> list[str]:
    cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL)
    return [re.sub(r'<[^>]+>', '', c).strip() for c in cells]


def _parse_num(text: str) -> float | None:
    clean = text.strip().replace('$', '').replace(',', '')
    if clean in ('N/A', '-', '', 'N/A '):
        return None
    try:
        return float(clean)
    except ValueError:
        return None


def _get_table_rows(table_html: str) -> list[list[str]]:
    raw_rows = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.DOTALL)
    return [_parse_cells(r) for r in raw_rows]


def _find_summary_val(rows: list[list[str]], label: str) -> str | None:
    for cells in rows:
        if len(cells) >= 2 and label.lower() in cells[0].lower():
            return cells[1]
    return None


def parse_analyst_data(html: str, current_price: float) -> dict:
    tables = re.findall(r'<table[^>]*>(.*?)</table>', html, re.DOTALL)
    result = {
        'target_high': None, 'target_low': None, 'target_median': None,
        'target_avg': None, 'num_ratings': None, 'upside_pct': None,
        'eps_fy1_avg': None, 'eps_fy2_avg': None,
        'eps_lq_est': None, 'eps_lq_act': None, 'eps_lq_surprise': None,
    }

    if len(tables) < 7:
        return result

    # Table 4 (index 3): Summary — Average Target Price, Number Of Ratings
    t4_rows = _get_table_rows(tables[3])
    target_avg_str = _find_summary_val(t4_rows, 'Average Target Price')
    num_ratings_str = _find_summary_val(t4_rows, 'Number Of Ratings')

    if target_avg_str:
        result['target_avg'] = _parse_num(target_avg_str)
    if num_ratings_str:
        result['num_ratings'] = _parse_num(num_ratings_str)

    # Table 5 (index 4): Target price range — High, Median, Low, Average
    t5_rows = _get_table_rows(tables[4])
    for cells in t5_rows:
        if len(cells) >= 2:
            label = cells[0].lower()
            val = _parse_num(cells[1])
            if 'high' in label:
                result['target_high'] = val
            elif 'median' in label:
                result['target_median'] = val
            elif 'low' in label and 'current' not in label:
                result['target_low'] = val

    # Sanity check: if target_avg is wildly different from current_price,
    # MarketWatch might be showing local-currency data (e.g. TSM in TWD)
    if result['target_avg'] and current_price > 0:
        ratio = result['target_avg'] / current_price
        if ratio > 5 or ratio < 0.2:
            # Likely currency mismatch — skip target prices
            result['target_high'] = None
            result['target_low'] = None
            result['target_median'] = None
            result['target_avg'] = None
            result['num_ratings'] = None

    # Calculate upside
    if result['target_avg'] and current_price > 0:
        result['upside_pct'] = round(((result['target_avg'] - current_price) / current_price) * 100, 2)

    # Table 6 (index 5): Annual EPS estimates
    # Row 0: ['', '2026', '2027', '2028', '2029']
    # Row 3: ['Average', '4.69', '8.15', '10.77', '13.24']
    t6_rows = _get_table_rows(tables[5])
    avg_row = None
    for cells in t6_rows:
        if len(cells) >= 3 and cells[0].lower() == 'average':
            avg_row = cells
            break
    if avg_row and len(avg_row) >= 3:
        result['eps_fy1_avg'] = _parse_num(avg_row[1]) if len(avg_row) > 1 else None
        result['eps_fy2_avg'] = _parse_num(avg_row[2]) if len(avg_row) > 2 else None

    # Table 7 (index 6): Quarterly EPS actual vs estimate
    # Row 0: ['', 'Q1 2026', 'Q2 2026', 'Q3 2026', 'Q4 2026']
    # Row 1: ['Estimate', ...]
    # Row 2: ['Actual', ...]
    # Row 3: ['Surprise', ...]
    # We want the LAST column (most recent completed quarter)
    t7_rows = _get_table_rows(tables[6])
    est_row = act_row = surp_row = None
    for cells in t7_rows:
        if len(cells) >= 2:
            label = cells[0].lower()
            if label == 'estimate':
                est_row = cells
            elif label == 'actual':
                act_row = cells
            elif label == 'surprise':
                surp_row = cells

    if est_row and len(est_row) >= 2:
        result['eps_lq_est'] = _parse_num(est_row[-1])
    if act_row and len(act_row) >= 2:
        result['eps_lq_act'] = _parse_num(act_row[-1])
    if surp_row and len(surp_row) >= 2:
        result['eps_lq_surprise'] = _parse_num(surp_row[-1])

    return result


def main():
    print(f"[{datetime.now()}] Starting MarketWatch analyst data collection...")
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:D'
    ).execute()
    rows = result.get('values', [])
    print(f"Found {len(rows)} tickers")

    updated_count = 0
    for i, row in enumerate(rows):
        ticker = row[0] if len(row) > 0 else ''
        current_price = float(row[3]) if len(row) > 3 and row[3] else 0
        row_num = i + 2

        if not ticker:
            continue

        print(f"[{i+1}/{len(rows)}] {ticker}...", end=' ')

        html = fetch_page(ticker)
        time.sleep(REQUEST_DELAY)

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if not html:
            row_data = [''] * 11 + [now]
        else:
            d = parse_analyst_data(html, current_price)
            row_data = [
                d['target_high'] if d['target_high'] is not None else '',
                d['target_low'] if d['target_low'] is not None else '',
                d['target_median'] if d['target_median'] is not None else '',
                d['target_avg'] if d['target_avg'] is not None else '',
                d['num_ratings'] if d['num_ratings'] is not None else '',
                d['upside_pct'] if d['upside_pct'] is not None else '',
                d['eps_fy1_avg'] if d['eps_fy1_avg'] is not None else '',
                d['eps_fy2_avg'] if d['eps_fy2_avg'] is not None else '',
                d['eps_lq_est'] if d['eps_lq_est'] is not None else '',
                d['eps_lq_act'] if d['eps_lq_act'] is not None else '',
                d['eps_lq_surprise'] if d['eps_lq_surprise'] is not None else '',
                now,
            ]
            target = d['target_avg']
            upside = d['upside_pct']
            if target:
                print(f"target=${target} upside={upside}% analysts={d['num_ratings']}")
            else:
                print(f"no target (EPS FY1={d['eps_fy1_avg']})")

        sheets.update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'StockUniverse!{COL_START}{row_num}:{COL_END}{row_num}',
            valueInputOption='RAW',
            body={'values': [row_data]}
        ).execute()
        updated_count += 1

    print(f"\nUpdated {updated_count} rows with MarketWatch data")
    print(f"[{datetime.now()}] Done!")


if __name__ == '__main__':
    main()
