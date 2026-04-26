#!/usr/bin/env python3
"""Daily MarketWatch analyst estimates collector. Writes to individual stock sheets.
Scrapes target prices + EPS estimates from MarketWatch analyst estimates pages.
"""
import os
import sys
import time
import re
import urllib.request
import urllib.error
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, get_sheets_service, get_stock_sheet_ids, get_header_map,
    find_or_create_today_row, write_stock_data, _is_retryable,
    get_trading_date, get_universe_ticker_rows, get_universe_header_map,
    write_universe_row,
)

MW_BASE = 'https://www.marketwatch.com/investing/stock'
REQUEST_DELAY = float(os.environ.get('MW_REQUEST_DELAY', '8.0'))

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Referer': 'https://www.google.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'Cache-Control': 'max-age=0',
}


def fetch_page(ticker: str) -> str | None:
    url = f"{MW_BASE}/{ticker.lower()}/analystestimates"
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status == 200:
                data = resp.read()
                if resp.headers.get('Content-Encoding') == 'gzip':
                    import gzip
                    data = gzip.decompress(data)
                return data.decode()
    except urllib.error.HTTPError as e:
        if e.code == 403:
            print("blocked", end=' ')
        elif e.code == 404:
            print("no page", end=' ')
        else:
            print(f"HTTP {e.code}", end=' ')
    except Exception as e:
        print(f"error: {e}", end=' ')
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
        'MW_Target_High': '', 'MW_Target_Low': '', 'MW_Target_Median': '',
        'MW_Target_Avg': '', 'MW_Num_Ratings': '', 'MW_Upside_Pct': '',
        'MW_EPS_FY1_Avg': '', 'MW_EPS_FY2_Avg': '',
        'MW_EPS_LQ_Est': '', 'EPS_LQ_Act': '', 'EPS_LQ_Surprise': '',
    }

    if len(tables) < 7:
        return result

    # Table 4 (index 3): Summary
    t4_rows = _get_table_rows(tables[3])
    target_avg_str = _find_summary_val(t4_rows, 'Average Target Price')
    num_ratings_str = _find_summary_val(t4_rows, 'Number Of Ratings')

    target_avg = _parse_num(target_avg_str) if target_avg_str else None
    if target_avg is not None:
        result['MW_Target_Avg'] = target_avg
    if num_ratings_str:
        v = _parse_num(num_ratings_str)
        if v is not None:
            result['MW_Num_Ratings'] = int(v)

    # Table 5 (index 4): Target price range
    t5_rows = _get_table_rows(tables[4])
    for cells in t5_rows:
        if len(cells) >= 2:
            label = cells[0].lower()
            val = _parse_num(cells[1])
            if val is None:
                continue
            if 'high' in label:
                result['MW_Target_High'] = val
            elif 'median' in label:
                result['MW_Target_Median'] = val
            elif 'low' in label and 'current' not in label:
                result['MW_Target_Low'] = val

    # Currency sanity check
    if target_avg and current_price > 0:
        ratio = target_avg / current_price
        if ratio > 5 or ratio < 0.2:
            result['MW_Target_High'] = ''
            result['MW_Target_Low'] = ''
            result['MW_Target_Median'] = ''
            result['MW_Target_Avg'] = ''
            result['MW_Num_Ratings'] = ''

    # Calculate upside
    if result['MW_Target_Avg'] and current_price > 0:
        result['MW_Upside_Pct'] = round(
            ((result['MW_Target_Avg'] - current_price) / current_price) * 100, 2
        )

    # Table 6 (index 5): Annual EPS estimates
    t6_rows = _get_table_rows(tables[5])
    for cells in t6_rows:
        if len(cells) >= 3 and cells[0].lower() == 'average':
            result['MW_EPS_FY1_Avg'] = _parse_num(cells[1]) if len(cells) > 1 else ''
            result['MW_EPS_FY2_Avg'] = _parse_num(cells[2]) if len(cells) > 2 else ''
            break

    # Table 7 (index 6): Quarterly EPS
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
        result['MW_EPS_LQ_Est'] = _parse_num(est_row[-1]) or ''
    if act_row and len(act_row) >= 2:
        result['EPS_LQ_Act'] = _parse_num(act_row[-1]) or ''
    if surp_row and len(surp_row) >= 2:
        result['EPS_LQ_Surprise'] = _parse_num(surp_row[-1]) or ''

    # Clean None→''
    for k, v in result.items():
        if v is None:
            result[k] = ''

    return result


def main():
    batch = os.environ.get('MW_BATCH', '')

    print(f"[{datetime.now(UTC8)}] Starting MarketWatch collection (batch={batch or 'all'})...")

    sheet_ids = get_stock_sheet_ids()
    if not sheet_ids:
        print("ERROR: No stock sheet IDs found.")
        return

    tickers = sorted(sheet_ids.keys())
    total = len(tickers)
    print(f"Loaded {total} stock sheet mappings")

    if batch == 'first':
        tickers = tickers[:250]
        print("Batch: first 250 tickers")
    elif batch == 'second':
        tickers = tickers[250:]
        print(f"Batch: last {len(tickers)} tickers")

    service = get_sheets_service()
    sheets = service.spreadsheets().values()
    today = get_trading_date()

    first_sid = sheet_ids[tickers[0]]
    header_map = get_header_map(sheets, first_sid)
    uni_header_map = get_universe_header_map(sheets)
    uni_ticker_rows = get_universe_ticker_rows(sheets)

    updated = 0
    for i, ticker in enumerate(tickers, 1):
        sid = sheet_ids[ticker]
        print(f"[{i}/{len(tickers)}] {ticker}...", end=' ', flush=True)

        html = fetch_page(ticker)
        time.sleep(REQUEST_DELAY)

        if not html:
            print()
            continue

        # Get current price from sheet for upside calc
        current_price = 0
        try:
            result = sheets.get(
                spreadsheetId=sid, range='Daily!J2:J'
            ).execute()
            price_rows = result.get('values', [])
            if price_rows:
                current_price = float(price_rows[-1][0])
        except Exception:
            pass

        data = parse_analyst_data(html, current_price)
        data['MW_Updated_At'] = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

        target = data.get('MW_Target_Avg', '')
        upside = data.get('MW_Upside_Pct', '')
        if target:
            print(f"target=${target} upside={upside}%")
        else:
            print(f"no target (EPS FY1={data.get('MW_EPS_FY1_Avg', '')})")

        try:
            row = find_or_create_today_row(sheets, sid, today)
            write_stock_data(sheets, sid, row, header_map, data)
            time.sleep(0.5)
            write_universe_row(sheets, uni_ticker_rows, uni_header_map, ticker, data)
            updated += 1
        except Exception as e:
            print(f"WRITE ERROR: {e}")

    print(f"\n[{datetime.now(UTC8)}] Done! Updated {updated}/{len(tickers)} sheets")


if __name__ == '__main__':
    main()
