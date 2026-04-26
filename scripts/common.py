"""Shared utilities for data collection scripts."""
import json
import os
import time
import warnings
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

UTC8 = timezone(timedelta(hours=8))
US_EASTERN = ZoneInfo('America/New_York')


def get_trading_date() -> str:
    """Return today's date in US Eastern time as YYYY-MM-DD.

    The stock market operates on Eastern Time, so when it's 4:30 AM in Taiwan
    (UTC+8) on 4/28, New York is still on 4/27 — data belongs in the 4/27 row.
    """
    return datetime.now(US_EASTERN).strftime('%Y-%m-%d')

CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'juns-stock-agent-5f32b75f7c83.json'))
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')
ROOT_FOLDER_ID = '1kpHXJlv4Abb_S6J8vTSUv44FOQEzDPMu'
CREATE_SHEETS_STATE_FILE = '/tmp/create_sheets_state.json'

ALL_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
]


def _get_creds():
    return service_account.Credentials.from_service_account_file(CREDS_FILE, scopes=ALL_SCOPES)


def get_sheets_service():
    return build('sheets', 'v4', credentials=_get_creds(), cache_discovery=False)


def get_drive_service():
    return build('drive', 'v3', credentials=_get_creds(), cache_discovery=False)


def sheets_update_with_retry(sheets, range_, values, value_input='RAW', retries=5):
    for attempt in range(retries):
        try:
            sheets.update(
                spreadsheetId=SPREADSHEET_ID, range=range_,
                valueInputOption=value_input, body={'values': values}
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"  Sheets error ({type(e).__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


RETRYABLE_ERRORS = ('429', 'Timeout', 'timed out', 'SSLEOFError', 'EOF occurred', 'ConnectionReset', 'BrokenPipe')


def _is_retryable(e: Exception) -> bool:
    msg = str(e)
    return any(s in msg or s in type(e).__name__ for s in RETRYABLE_ERRORS)


def sheets_append_with_retry(sheets, range_, values, retries=5, batch_size=50):
    for i in range(0, len(values), batch_size):
        chunk = values[i:i + batch_size]
        for attempt in range(retries):
            try:
                sheets.append(
                    spreadsheetId=SPREADSHEET_ID, range=range_,
                    valueInputOption='RAW', insertDataOption='INSERT_ROWS',
                    body={'values': chunk}
                ).execute()
                break
            except Exception as e:
                if attempt < retries - 1 and _is_retryable(e):
                    wait = 30 * (attempt + 1)
                    print(f"  Sheets error ({type(e).__name__}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        time.sleep(2)


# ---------------------------------------------------------------------------
# Stock sheet helpers — individual per-ticker spreadsheets in /stocks folder
# ---------------------------------------------------------------------------

def col_letter(n: int) -> str:
    """Convert 1-based column number to letter(s). 1=A, 27=AA, 170=FN."""
    result = ''
    while n > 0:
        n -= 1
        result = chr(65 + n % 26) + result
        n //= 26
    return result


def get_stock_sheet_ids() -> dict[str, str]:
    """Return {ticker: spreadsheet_id} mapping.

    Reads from local state file (dev) or SheetMapping tab (GitHub Actions).
    """
    if os.path.exists(CREATE_SHEETS_STATE_FILE):
        with open(CREATE_SHEETS_STATE_FILE) as f:
            return json.load(f).get('created', {})
    sheets = get_sheets_service().spreadsheets().values()
    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID, range='SheetMapping!A2:B'
    ).execute()
    return {r[0]: r[1] for r in result.get('values', []) if len(r) >= 2}


def get_header_map(sheets_values, spreadsheet_id: str,
                    tab_name: str = 'Daily') -> dict[str, int]:
    """Read header row, return {column_name: 1-based index}."""
    result = sheets_values.get(
        spreadsheetId=spreadsheet_id, range=f'{tab_name}!1:1'
    ).execute()
    headers = result.get('values', [[]])[0]
    return {name: i + 1 for i, name in enumerate(headers)}


def find_or_create_today_row(sheets_values, spreadsheet_id: str,
                              today_str: str) -> int:
    """Find today's row in column A, or append a new one. Returns row number."""
    result = sheets_values.get(
        spreadsheetId=spreadsheet_id, range='Daily!A:A'
    ).execute()
    values = result.get('values', [])
    for i in range(len(values) - 1, 0, -1):
        if values[i] and values[i][0] == today_str:
            return i + 1
    next_row = len(values) + 1
    sheets_values.update(
        spreadsheetId=spreadsheet_id, range=f'Daily!A{next_row}',
        valueInputOption='RAW', body={'values': [[today_str]]}
    ).execute()
    return next_row


def read_existing_row(sheets_values, spreadsheet_id: str, row_num: int,
                      header_map: dict[str, int],
                      tab_name: str = 'Daily') -> dict[str, str]:
    """Read existing values for a row, return {column_name: value} for non-empty cells."""
    if not header_map:
        return {}
    max_col = max(header_map.values())
    result = sheets_values.get(
        spreadsheetId=spreadsheet_id,
        range=f'{tab_name}!A{row_num}:{col_letter(max_col)}{row_num}',
    ).execute()
    row_vals = result.get('values', [[]])[0] if result.get('values') else []
    existing = {}
    for name, idx in header_map.items():
        i = idx - 1
        if i < len(row_vals) and row_vals[i] not in ('', None):
            existing[name] = row_vals[i]
    return existing


def write_stock_data(sheets_values, spreadsheet_id: str, row_num: int,
                     header_map: dict[str, int], data: dict,
                     tab_name: str = 'Daily', retries: int = 5) -> None:
    """Write data dict to specific columns of a row via batchUpdate.

    data: {column_name: value, ...}
    header_map: {column_name: 1-based column index}
    """
    ranges = []
    for col_name, value in data.items():
        idx = header_map.get(col_name)
        if idx is None:
            continue
        cell = f"{tab_name}!{col_letter(idx)}{row_num}"
        ranges.append({'range': cell, 'values': [[value]]})
    if not ranges:
        return
    for attempt in range(retries):
        try:
            sheets_values.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={'valueInputOption': 'RAW', 'data': ranges},
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"    Sheets write retry ({type(e).__name__}), {wait}s...")
                time.sleep(wait)
            else:
                raise


def round_if(val, digits=2):
    """Round numeric value, pass through empty strings."""
    if val == '' or val is None:
        return ''
    try:
        return round(float(val), digits)
    except (TypeError, ValueError):
        return val


def api_retry(fn, *args, retries=5, **kwargs):
    """Retry an API call with backoff."""
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


# ---------------------------------------------------------------------------
# StockUniverse helpers — latest snapshot in the main spreadsheet
# ---------------------------------------------------------------------------

def get_universe_ticker_rows(sheets_values) -> dict[str, int]:
    """Return {ticker: row_number} for StockUniverse tab."""
    result = sheets_values.get(
        spreadsheetId=SPREADSHEET_ID, range='StockUniverse!A2:A'
    ).execute()
    rows = result.get('values', [])
    return {r[0]: i + 2 for i, r in enumerate(rows) if r}


def get_universe_header_map(sheets_values) -> dict[str, int]:
    """Return {column_name: 1-based index} for StockUniverse tab."""
    return get_header_map(sheets_values, SPREADSHEET_ID, tab_name='StockUniverse')


def write_universe_row(sheets_values, ticker_rows: dict[str, int],
                       universe_header_map: dict[str, int],
                       ticker: str, data: dict) -> None:
    """Write latest data for a ticker to StockUniverse."""
    row = ticker_rows.get(ticker)
    if row is None:
        return
    write_stock_data(sheets_values, SPREADSHEET_ID, row,
                     universe_header_map, data, tab_name='StockUniverse')


def batch_write_universe(sheets_values, ticker_rows: dict[str, int],
                         universe_header_map: dict[str, int],
                         batch: list[tuple[str, dict]],
                         retries: int = 5) -> None:
    """Write multiple tickers' data to StockUniverse in one batchUpdate."""
    all_ranges = []
    for ticker, data in batch:
        row = ticker_rows.get(ticker)
        if row is None:
            continue
        for col_name, value in data.items():
            idx = universe_header_map.get(col_name)
            if idx is None:
                continue
            cell = f"StockUniverse!{col_letter(idx)}{row}"
            all_ranges.append({'range': cell, 'values': [[value]]})
    if not all_ranges:
        return
    for attempt in range(retries):
        try:
            sheets_values.batchUpdate(
                spreadsheetId=SPREADSHEET_ID,
                body={'valueInputOption': 'RAW', 'data': all_ranges},
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"    Universe batch write retry ({type(e).__name__}), {wait}s...")
                time.sleep(wait)
            else:
                raise
