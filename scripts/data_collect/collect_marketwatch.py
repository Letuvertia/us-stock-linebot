#!/usr/bin/env python3
"""Daily MarketWatch analyst estimates collector. Writes to individual stock sheets.
Scrapes target prices + EPS estimates from MarketWatch analyst estimates pages.
Uses nodriver (undetected Chrome) to bypass DataDome bot detection.
"""
import argparse
import asyncio
import json
import os
import random
import sys
import time
import re
from datetime import datetime

import nodriver as uc

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, get_sheets_service, get_stock_sheet_ids, get_header_map,
    find_or_create_today_row, write_stock_data, _is_retryable,
    get_trading_date, get_universe_ticker_rows, get_universe_header_map,
    batch_write_universe,
)

MW_BASE = 'https://www.marketwatch.com/investing/stock'
REQUEST_DELAY_LO = float(os.environ.get('MW_REQUEST_DELAY_LO', '8.0'))
REQUEST_DELAY_HI = float(os.environ.get('MW_REQUEST_DELAY_HI', '15.0'))
BACKOFF_PAUSE = float(os.environ.get('MW_BACKOFF_PAUSE', '600.0'))
MAX_CONSECUTIVE_BLOCKS = int(os.environ.get('MW_MAX_BLOCKS', '5'))
MAX_RESTART_CYCLES = int(os.environ.get('MW_MAX_RESTARTS', '3'))

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROGRESS_FILE = os.path.join(_SCRIPT_DIR, '.mw_progress.json')


def _load_progress() -> str | None:
    try:
        with open(_PROGRESS_FILE) as f:
            return json.load(f).get('last_ticker')
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return None


def _save_progress(ticker: str) -> None:
    with open(_PROGRESS_FILE, 'w') as f:
        json.dump({'last_ticker': ticker, 'updated_at': datetime.now(UTC8).isoformat()}, f)


async def _start_browser():
    config = uc.Config()
    config.headless = False
    browser = await uc.start(config=config)
    return browser


async def _warmup(browser) -> bool:
    try:
        tab = await browser.get('https://www.marketwatch.com/')
        await asyncio.sleep(random.uniform(3.0, 5.0))
        title = await tab.evaluate('document.title')
        if title and 'marketwatch' in title.lower() and title != 'marketwatch.com':
            return True
        src = await tab.get_content()
        if 'datadome' not in src.lower() and len(src) > 5000:
            return True
        print(f"WARNING: homepage may be blocked (title={title})")
    except Exception as e:
        print(f"WARNING: homepage warmup failed: {e}")
    return False


async def fetch_page(browser, ticker: str) -> str | None:
    url = f"{MW_BASE}/{ticker.lower()}/analystestimates"
    try:
        tab = await browser.get(url)
        await asyncio.sleep(random.uniform(2.0, 4.0))

        src = await tab.get_content()
        if 'datadome' in src.lower() and len(src) < 3000:
            print("blocked", end=' ')
            return 'BLOCKED'

        try:
            await tab.select('table', timeout=10)
        except Exception:
            if len(src) < 5000:
                print("no tables", end=' ')
                return None

        html = await tab.get_content()
        tables = re.findall(r'<table', html)
        if len(tables) >= 7:
            return html

        if len(tables) == 0:
            print("no tables", end=' ')
        else:
            print(f"{len(tables)} tables (need 7)", end=' ')
        return html if len(tables) > 0 else None

    except Exception as e:
        msg = str(e)
        if 'Timeout' in msg or 'timeout' in msg:
            print("timeout", end=' ')
        else:
            print(f"error: {msg[:80]}", end=' ')
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

    if target_avg and current_price > 0:
        ratio = target_avg / current_price
        if ratio > 5 or ratio < 0.2:
            result['MW_Target_High'] = ''
            result['MW_Target_Low'] = ''
            result['MW_Target_Median'] = ''
            result['MW_Target_Avg'] = ''
            result['MW_Num_Ratings'] = ''

    if result['MW_Target_Avg'] and current_price > 0:
        result['MW_Upside_Pct'] = round(
            ((result['MW_Target_Avg'] - current_price) / current_price) * 100, 2
        )

    t6_rows = _get_table_rows(tables[5])
    for cells in t6_rows:
        if len(cells) >= 3 and cells[0].lower() == 'average':
            result['MW_EPS_FY1_Avg'] = _parse_num(cells[1]) if len(cells) > 1 else ''
            result['MW_EPS_FY2_Avg'] = _parse_num(cells[2]) if len(cells) > 2 else ''
            break

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

    for k, v in result.items():
        if v is None:
            result[k] = ''

    return result


async def async_main(n: int | None = None):
    print(f"[{datetime.now(UTC8)}] Starting MarketWatch collection (n={n or 'all'})...")

    sheet_ids = get_stock_sheet_ids()
    if not sheet_ids:
        print("ERROR: No stock sheet IDs found.")
        return

    tickers = sorted(sheet_ids.keys())
    total = len(tickers)
    print(f"Loaded {total} stock sheet mappings")

    last_ticker = _load_progress()
    if last_ticker and last_ticker in tickers:
        skip_idx = tickers.index(last_ticker) + 1
        if skip_idx < len(tickers):
            print(f"Resuming after {last_ticker} (skipping {skip_idx} already-done tickers)")
            tickers = tickers[skip_idx:]
        else:
            print(f"All tickers already done (last={last_ticker}). Re-running from start.")

    if n is not None:
        tickers = tickers[:n]
        print(f"Batch: next {n} tickers")

    service = get_sheets_service()
    sheets = service.spreadsheets().values()
    today = get_trading_date()

    first_sid = sheet_ids[tickers[0]]
    header_map = get_header_map(sheets, first_sid)
    uni_header_map = get_universe_header_map(sheets)
    uni_ticker_rows = get_universe_ticker_rows(sheets)

    browser = await _start_browser()
    warm = await _warmup(browser)
    if warm:
        print("Browser launched, homepage warmup OK")
    else:
        print("Browser launched, homepage warmup FAILED (will try anyway)")

    UNIVERSE_BATCH_SIZE = 20
    universe_buffer: list[tuple[str, dict]] = []
    updated = 0
    consecutive_blocks = 0
    restart_cycles = 0

    try:
        for i, ticker in enumerate(tickers, 1):
            sid = sheet_ids[ticker]
            print(f"[{i}/{len(tickers)}] {ticker}...", end=' ', flush=True)

            html = await fetch_page(browser, ticker)
            await asyncio.sleep(random.uniform(REQUEST_DELAY_LO, REQUEST_DELAY_HI))

            if html == 'BLOCKED':
                consecutive_blocks += 1
                print()
                if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                    restart_cycles += 1
                    if restart_cycles >= MAX_RESTART_CYCLES:
                        print(f"  {restart_cycles} restart cycles exhausted — "
                              f"stopping. Resume later to continue.")
                        break
                    print(f"  {consecutive_blocks} consecutive blocks — "
                          f"pausing {BACKOFF_PAUSE}s and restarting browser "
                          f"(cycle {restart_cycles}/{MAX_RESTART_CYCLES})...")
                    browser.stop()
                    await asyncio.sleep(BACKOFF_PAUSE)
                    browser = await _start_browser()
                    warm = await _warmup(browser)
                    print(f"  Browser restarted (warmup={'OK' if warm else 'FAIL'})")
                    consecutive_blocks = 0
                continue

            if not html:
                print()
                continue

            consecutive_blocks = 0

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
                updated += 1
                _save_progress(ticker)
            except Exception as e:
                print(f"WRITE ERROR: {e}")
                continue

            universe_buffer.append((ticker, data))
            if len(universe_buffer) >= UNIVERSE_BATCH_SIZE:
                try:
                    batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
                    print(f"  [universe batch: {len(universe_buffer)} tickers written]")
                except Exception as e:
                    print(f"  [universe batch WRITE ERROR: {e}]")
                universe_buffer = []
                await asyncio.sleep(2)

        if universe_buffer:
            try:
                batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
                print(f"  [universe batch: {len(universe_buffer)} tickers written]")
            except Exception as e:
                print(f"  [universe batch WRITE ERROR: {e}]")

    finally:
        browser.stop()

    print(f"\n[{datetime.now(UTC8)}] Done! Updated {updated}/{len(tickers)} sheets")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', type=int, default=None,
                        help='Number of tickers to fetch from last updated position')
    args = parser.parse_args()
    uc.loop().run_until_complete(async_main(n=args.n))


if __name__ == '__main__':
    main()
