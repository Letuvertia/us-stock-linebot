"""Summarize 代代財女 transcripts using local Ollama.

Cron: 5 11 * * * (daily at 11:05 UTC = 19:05 UTC+8, after transcription)
Reads rows where LocalTXT is set and Summary (col K) is empty.
Calls Ollama with JSON-format output, formats into bullet list, writes to col K,
then POSTs podcast_summarized to GAS to trigger a LINE push.
"""
import argparse
import json
import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8
from config import OLLAMA_MODEL
from podcast_common import (
    get_podcast_sheets_service,
    get_podcast_spreadsheet_id,
    sheets_update_with_retry,
)

REPO_ROOT = Path(__file__).resolve().parents[3]

COL_TITLE = 2
COL_EPISODE_URL = 5
COL_LOCAL_TXT = 9
COL_SUMMARY = 10

PROMPT_TEMPLATE = """\
你是一位專業股票研究員（Sell-side Equity Analyst）。

請閱讀以下 Podcast 逐字稿，忽略所有閒聊、業配、個人故事與非投資內容。

請特別關注：個股、公司、產業、漲價、缺貨、擴產、AI供應鏈、法說會、財報、毛利率、EPS、資金流向。

若逐字稿完全沒有投資相關內容，所有欄位填空陣列。

請嚴格輸出以下 JSON 格式，不要輸出任何其他文字：

{{
  "core_thesis": ["最重要的投資觀點，最多3點，每點一句話"],
  "stocks": [
    {{"name": "公司或產業名稱", "sentiment": "偏多/偏空/中立", "reason": "理由", "catalyst": "催化劑"}}
  ],
  "indicators": ["未來1~3個月可觀察的驗證指標，最多3點"]
}}

逐字稿：

{transcript}
"""


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


def _format_summary(data: dict) -> str:
    lines = []

    thesis = [t.strip() for t in data.get('core_thesis', []) if t.strip()]
    if thesis:
        lines.append('【核心觀點】')
        lines.extend(f'- {t}' for t in thesis)

    stocks = [s for s in data.get('stocks', []) if s.get('name')]
    if stocks:
        lines.append('【個股與產業】')
        for s in stocks:
            sentiment = s.get('sentiment', '')
            reason = s.get('reason', '')
            catalyst = s.get('catalyst', '')
            detail = ' — '.join(p for p in [reason, catalyst] if p)
            lines.append(f'- {s["name"]}（{sentiment}）：{detail}' if detail else f'- {s["name"]}（{sentiment}）')

    indicators = [i.strip() for i in data.get('indicators', []) if i.strip()]
    if indicators:
        lines.append('【驗證指標】')
        lines.extend(f'- {i}' for i in indicators)

    if not lines:
        return '本集無投資相關內容'

    return '\n'.join(lines)


def _summarize(transcript: str) -> str | None:
    base_url = _ollama_base_url()
    prompt = PROMPT_TEMPLATE.format(transcript=transcript)
    raw = ''
    try:
        resp = requests.post(
            f'{base_url}/api/generate',
            json={
                'model': OLLAMA_MODEL,
                'prompt': prompt,
                'stream': False,
                'think': False,
                'format': 'json',
                'options': {'temperature': 0.3, 'num_ctx': 32768},
            },
        )
        resp.raise_for_status()
        raw = resp.json().get('response', '').strip()
        data = json.loads(raw)
        return _format_summary(data)
    except json.JSONDecodeError as e:
        print(f'    ✗ JSON parse error: {e} — raw: {raw[:200]}')
        return None
    except Exception as e:
        print(f'    ✗ Ollama error: {e}')
        return None


def _notify_gas(episode_id: str, title: str, summary: str, episode_url: str) -> None:
    url = os.environ.get('GAS_WEBHOOK_URL', '')
    if not url:
        print('  GAS_WEBHOOK_URL not set, skipping notification')
        return
    try:
        resp = requests.post(
            url,
            json={
                'event': 'podcast_summarized',
                'id': episode_id,
                'title': title,
                'summary': summary,
                'episode_url': episode_url,
            },
            timeout=30,
        )
        print(f'  GAS notified: {resp.status_code}')
    except Exception as e:
        print(f'  GAS notification failed: {e}')


def _process_row(sheets, sid: str, sheet_row: int, row: list, txt_path: Path) -> bool:
    title = row[COL_TITLE] if len(row) > COL_TITLE else txt_path.stem
    episode_id = row[0] if row else ''
    episode_url = row[COL_EPISODE_URL] if len(row) > COL_EPISODE_URL else ''

    print(f'\n  [{sheet_row}] {title[:70]}', flush=True)

    transcript = txt_path.read_text(encoding='utf-8').strip()
    if not transcript:
        print('    transcript file is empty, skipping')
        return False

    print(f'    transcript: {len(transcript)} chars', flush=True)
    summary = _summarize(transcript)
    if not summary:
        return False

    print(f'    ✓ {len(summary)} chars')
    sheets_update_with_retry(
        sheets, sid, f'Sheet1!K{sheet_row}',
        [[summary]],
    )
    _notify_gas(episode_id, title, summary, episode_url)
    return True


def main():
    print(f'[{datetime.now(UTC8)}] Starting 代代財女 summarization...')

    sid = get_podcast_spreadsheet_id('RichWomen')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    result = sheets.get(spreadsheetId=sid, range='Sheet1!A2:K').execute()
    rows = result.get('values', [])
    if not rows:
        print('No episodes in sheet')
        return

    to_summarize = []
    for i, row in enumerate(rows):
        if len(row) <= COL_LOCAL_TXT or not row[COL_LOCAL_TXT].strip():
            continue
        if len(row) > COL_SUMMARY and row[COL_SUMMARY].strip():
            continue
        txt_rel = row[COL_LOCAL_TXT].strip()
        txt_path = REPO_ROOT / txt_rel
        if not txt_path.exists():
            print(f'  [row {i+2}] TXT not found on disk: {txt_rel}, skipping')
            continue
        to_summarize.append((i + 2, row, txt_path))

    print(f'Found {len(to_summarize)} episode(s) to summarize')
    if not to_summarize:
        return

    summarized = sum(
        1 for sheet_row, row, txt_path in to_summarize
        if _process_row(sheets, sid, sheet_row, row, txt_path)
        or not time.sleep(2)
    )

    print(f'\nDone. Summarized {summarized}/{len(to_summarize)} episode(s)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ep', help='Episode title fragment — looks up row from sheet')
    args = parser.parse_args()

    sid = get_podcast_spreadsheet_id('RichWomen')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    if args.ep:
        rows = sheets.get(spreadsheetId=sid, range='Sheet1!A2:K').execute().get('values', [])
        match = next(
            ((i + 2, row) for i, row in enumerate(rows)
             if len(row) > COL_TITLE and args.ep in row[COL_TITLE]
             and len(row) > COL_LOCAL_TXT and row[COL_LOCAL_TXT].strip()),
            None,
        )
        if not match:
            print(f"No sheet row found for '{args.ep}' with a LocalTXT path")
            sys.exit(1)
        sheet_row, row = match
        txt_path = REPO_ROOT / row[COL_LOCAL_TXT].strip()
        if not txt_path.exists():
            print(f'TXT not found on disk: {txt_path}')
            sys.exit(1)
        _process_row(sheets, sid, sheet_row, row, txt_path)
    else:
        main()
