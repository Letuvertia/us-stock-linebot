#!/usr/bin/env python3
"""Daily FMP price target collector. Writes to individual stock sheets.
Free tier: 250 calls/day per key. Multiple keys rotated round-robin.
"""
import os
import sys
import time
import json
import urllib.request
import urllib.error
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, get_sheets_service, get_stock_sheet_ids, get_header_map,
    find_or_create_today_row, write_stock_data, round_if, _is_retryable,
    get_trading_date, get_universe_ticker_rows, get_universe_header_map,
    batch_write_universe,
)

FMP_KEYS = [k.strip() for k in os.environ['FMP_API_KEY'].split(',') if k.strip()]
FMP_BASE = 'https://financialmodelingprep.com/stable'
_fmp_key_index = 0
_fmp_dead_keys = set()


def fmp_get(endpoint: str) -> list | None:
    global _fmp_key_index
    if len(_fmp_dead_keys) >= len(FMP_KEYS):
        print("all keys exhausted!", end=' ')
        return None
    for _ in range(len(FMP_KEYS)):
        while FMP_KEYS[_fmp_key_index % len(FMP_KEYS)] in _fmp_dead_keys:
            _fmp_key_index += 1
            if len(_fmp_dead_keys) >= len(FMP_KEYS):
                return None
        key = FMP_KEYS[_fmp_key_index % len(FMP_KEYS)]
        url = f"{FMP_BASE}/{endpoint}{'&' if '?' in endpoint else '?'}apikey={key}"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                key_num = _fmp_key_index % len(FMP_KEYS) + 1
                _fmp_dead_keys.add(key)
                print(f"key {key_num} rate limited, switching...", end=' ', flush=True)
                _fmp_key_index += 1
                continue
            print(f"HTTP {e.code}", end=' ')
        except Exception as e:
            print(f"error: {e}", end=' ')
        return None
    return None


def main():
    batch = os.environ.get('FMP_BATCH', '')

    print(f"[{datetime.now(UTC8)}] Starting FMP target collection (batch={batch or 'all'})...")

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

    WRITE_INTERVAL = 2.0
    UNIVERSE_BATCH_SIZE = 100
    last_write_time = 0.0
    universe_buffer = []
    updated = 0

    for i, ticker in enumerate(tickers, 1):
        sid = sheet_ids[ticker]
        print(f"[{i}/{len(tickers)}] {ticker}...", end=' ', flush=True)

        fmp_data = fmp_get(f'price-target-consensus?symbol={ticker}')

        now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

        has_data = fmp_data and len(fmp_data) > 0

        if not has_data:
            print("no target data")
            universe_buffer.append((ticker, {'FMP_Updated_At': now}))
            continue

        d = fmp_data[0]
        target_consensus = d.get('targetConsensus', '')

        upside = ''
        try:
            result = sheets.get(
                spreadsheetId=sid, range='Daily!J2:J'
            ).execute()
            price_rows = result.get('values', [])
            if price_rows:
                last_price = float(price_rows[-1][0])
                if target_consensus and last_price > 0:
                    upside = round(((target_consensus - last_price) / last_price) * 100, 2)
        except Exception:
            pass

        data = {
            'FMP_Target_High': d.get('targetHigh', ''),
            'FMP_Target_Low': d.get('targetLow', ''),
            'FMP_Target_Consensus': target_consensus,
            'FMP_Target_Median': d.get('targetMedian', ''),
            'FMP_Upside_Pct': upside,
            'FMP_Updated_At': now,
        }
        print(f"consensus=${target_consensus} upside={upside}%", end=' ', flush=True)

        elapsed = time.monotonic() - last_write_time
        if elapsed < WRITE_INTERVAL:
            time.sleep(WRITE_INTERVAL - elapsed)

        try:
            row = find_or_create_today_row(sheets, sid, today)
            write_stock_data(sheets, sid, row, header_map, data)
            last_write_time = time.monotonic()
            universe_buffer.append((ticker, data))
            print(f"→ row {row}")
            updated += 1
        except Exception as e:
            print(f"WRITE ERROR: {e}")

        if len(universe_buffer) >= UNIVERSE_BATCH_SIZE:
            batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
            last_write_time = time.monotonic()
            print(f"  [universe batch {len(universe_buffer)} written]", flush=True)
            universe_buffer = []

    if universe_buffer:
        elapsed = time.monotonic() - last_write_time
        if elapsed < WRITE_INTERVAL:
            time.sleep(WRITE_INTERVAL - elapsed)
        batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
        print(f"  [universe batch {len(universe_buffer)} written]")

    print(f"\n[{datetime.now(UTC8)}] Done! Updated {updated}/{len(tickers)} sheets")


if __name__ == '__main__':
    main()
