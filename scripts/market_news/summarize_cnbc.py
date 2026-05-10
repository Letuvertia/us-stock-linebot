"""Summarize recent CNBC news using local Ollama and write to 'Summary' column.

Cron: 5 * * * * (every hour at xx:05)
Processes all rows from the last 7 days with an empty Summary column.
"""
import json
import sys
import os
import time
import random
import subprocess
from datetime import datetime, timedelta

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import data_common
from config import OLLAMA_MODEL, LOOKBACK_DAYS

SUMMARY_COL_HEADER = 'Summary'
MAX_CONTENT_CHARS = 3000

PROMPT_TEMPLATE = """你是一位專業金融分析師。請閱讀以下新聞，並嚴格按照指定的 JSON 格式輸出分析結果。

【任務規範】：
1. **summary**：繁體中文摘要，嚴格限制在 150 字以內。
   - 格式：以 [關鍵詞] 開頭，包含 3 個短句：核心事件、當前影響、未來風險。
2. **related_stocks**：列出 2-3 檔最相關的 S&P 500 股票標的。
   - 屬性：ticker (代碼), reason (關聯原因), impact (利多/利空/中立)。
3. **語言**：使用台灣繁體中文金融用語（如：營收、地緣政治、避險）。
4. **輸出格式**：僅輸出 JSON 格式，不要有任何 Markdown 代碼塊、解釋或思考過程。

【JSON 結構範例】：
{{
  "summary": "[地緣政治] 美伊衝突升級導致原油供應疑慮。目前油價飆升推升通膨預期。未來需關注能源出口國之制裁風險。",
  "related_stocks": [
    {{
      "ticker": "XOM",
      "reason": "原油價格上漲利於油氣開發商營收",
      "impact": "利多"
    }},
    {{
      "ticker": "DAL",
      "reason": "油價成本攀升增加航空公司營運壓力",
      "impact": "利空"
    }}
  ]
}}

---
新聞內容：
{content}"""


def _wsl_host() -> str:
    try:
        out = subprocess.check_output(['ip', 'route', 'show', 'default'], text=True)
        for part in out.split():
            if part not in ('default', 'via', 'dev', 'proto', 'kernel', 'src'):
                return part
    except Exception:
        pass
    return 'host.docker.internal'


def _ollama_base_url() -> str:
    for host in ('localhost', _wsl_host()):
        url = f'http://{host}:11434'
        try:
            requests.get(f'{url}/api/tags', timeout=2)
            return url
        except Exception:
            continue
    return 'http://localhost:11434'



def _parse_result(data: dict) -> tuple[str, list[str]]:
    """Return (summary_text, [tickers]) from parsed LLM JSON.
    Prices are omitted — GAS injects live prices at report time."""
    parts = []
    tickers = []

    summary = data.get('summary', '').strip()
    if summary:
        parts.append(summary)

    stocks = data.get('related_stocks', [])
    for s in stocks:
        ticker = s.get('ticker', '').strip().upper()
        reason = s.get('reason', '').strip()
        impact = s.get('impact', '').strip()
        if not ticker or not impact:
            continue
        tickers.append(ticker)
        entry = f'{impact}: {ticker} - {reason}' if reason else f'{impact}: {ticker}'
        parts.append(entry)

    return '\n'.join(parts), tickers


def _summarize(title: str, content: str) -> tuple[str, list[str]] | None:
    """Return (summary_text, tickers) or None on failure."""
    full_text = f'{title}\n\n{content}' if title else content
    prompt = PROMPT_TEMPLATE.format(content=full_text[:MAX_CONTENT_CHARS])
    base_url = _ollama_base_url()
    raw = ''
    try:
        resp = requests.post(f'{base_url}/api/generate', json={
            'model': OLLAMA_MODEL,
            'prompt': prompt,
            'stream': False,
            'think': False,
            'format': 'json',
            'options': {'temperature': 0.3},
        }, timeout=120)
        resp.raise_for_status()
        raw = resp.json().get('response', '').strip()
        data = json.loads(raw)
        return _parse_result(data)
    except json.JSONDecodeError as e:
        print(f'  JSON parse error: {e} — raw: {raw[:100]}')
        return None
    except Exception as e:
        print(f'  Ollama error: {e}')
        return None


def _sheets_write(sheets, cnbc_id: str, range_: str, values: list, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            sheets.update(
                spreadsheetId=cnbc_id,
                range=range_,
                valueInputOption='RAW',
                body={'values': values},
            ).execute()
            return
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt + random.uniform(0, 1)
            print(f'    Sheets write error ({e}), retry in {wait:.1f}s...')
            time.sleep(wait)


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to spreadsheet letter (A, B, …, Z, AA, AB, …)."""
    result = ''
    idx += 1  # 1-based
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        result = chr(ord('A') + rem) + result
    return result


def main():
    svc = data_common.get_sheets_service()
    sheets = svc.spreadsheets().values()
    cnbc_id = data_common.get_news_sheet_ids()['CNBC']

    result = sheets.get(spreadsheetId=cnbc_id, range='Sheet1!A:H').execute()
    rows = result.get('values', [])
    if not rows:
        print('Sheet is empty')
        return

    header = list(rows[0])

    if SUMMARY_COL_HEADER in header:
        summary_col = header.index(SUMMARY_COL_HEADER)
    else:
        summary_col = len(header)
        col_ltr = _col_letter(summary_col)
        _sheets_write(sheets, cnbc_id, f'Sheet1!{col_ltr}1', [[SUMMARY_COL_HEADER]])
        print(f'Added "{SUMMARY_COL_HEADER}" header at column {col_ltr}')

    col_ltr = _col_letter(summary_col)
    ticker_tag_col_ltr = 'C'  # TickerTags column is always C (index 2)
    data = rows[1:]
    cutoff_date = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date()

    to_process = []
    for i, row in enumerate(data):
        date_str = row[1] if len(row) > 1 else ''
        content = row[4] if len(row) > 4 else ''
        existing = row[summary_col] if len(row) > summary_col else ''

        if not content.strip() or not date_str:
            continue
        if existing.strip():
            continue

        try:
            row_date = datetime.strptime(date_str[:10], '%Y-%m-%d').date()
        except ValueError:
            continue
        if row_date < cutoff_date:
            continue

        to_process.append((i + 2, row))

    print(f'Found {len(to_process)} rows to summarize (out of {len(data)} total)')

    updated = 0
    updated_ids = []
    for idx, (sheet_row, row) in enumerate(to_process, 1):
        title = row[3] if len(row) > 3 else ''
        content = row[4] if len(row) > 4 else ''
        slug = title[:70] if title else content[:70]
        print(f'  [{idx}/{len(to_process)}] Row {sheet_row}: {slug}')

        result = _summarize(title, content)
        if result:
            summary, llm_tickers = result
            _sheets_write(sheets, cnbc_id, f'Sheet1!{col_ltr}{sheet_row}', [[summary]])
            if llm_tickers:
                existing_tags = row[2] if len(row) > 2 else ''
                existing_set = {t.strip() for t in existing_tags.split(',') if t.strip()}
                merged = ','.join(sorted(existing_set | set(llm_tickers)))
                _sheets_write(sheets, cnbc_id, f'Sheet1!{ticker_tag_col_ltr}{sheet_row}', [[merged]])
            updated += 1
            news_id = row[0] if len(row) > 0 else ''
            if news_id:
                updated_ids.append(str(news_id))
            tag_info = f', tags: {",".join(llm_tickers)}' if llm_tickers else ''
            print(f'    ✓ written ({len(summary)} chars{tag_info})')
        else:
            print(f'    ✗ failed, skipping')

        time.sleep(random.uniform(0.3, 0.8))

    print(f'\nDone. Updated {updated}/{len(to_process)} rows.')

    if updated_ids:
        _notify_gas(updated_ids)


def _notify_gas(ids: list) -> None:
    url = data_common.get_config('GAS_WEBHOOK_URL')
    if not url:
        print('  GAS_WEBHOOK_URL not in Config tab, skipping notification')
        return
    try:
        resp = requests.post(
            url,
            json={'event': 'summaries_updated', 'ids': ids},
            timeout=30,
        )
        print(f'  GAS notified: {resp.status_code} ({len(ids)} IDs)')
    except Exception as e:
        print(f'  GAS notification failed: {e}')


if __name__ == '__main__':
    main()
