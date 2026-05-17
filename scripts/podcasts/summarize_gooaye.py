"""Summarize Gooaye 股癌 transcripts using local Ollama.

Cron: 0 11 * * * (daily at 11:00 UTC = 19:00 UTC+8, after transcription)
Reads rows where LocalTXT is set and Summary (col K) is empty.
Calls Ollama with a finance-focused prompt, writes the summary back to col K,
then POSTs podcast_summarized to GAS to trigger a LINE push.
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8
from config import OLLAMA_MODEL
from podcast_common import (
    get_podcast_sheets_service,
    get_podcast_spreadsheet_id,
    sheets_update_with_retry,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Column indices (0-based in values list)
COL_TITLE = 2
COL_EPISODE_URL = 5
COL_LOCAL_TXT = 9
COL_SUMMARY = 10

PROMPT_TEMPLATE = """\
你是一個專業的財經 podcast 摘要助手。

請閱讀以下逐字稿內容，並僅整理「與股票、投資、金融市場、總體經濟、公司營運、產業趨勢」相關的資訊。

忽略以下內容：
- 主持人閒聊
- 個人生活故事
- 情緒抒發
- 玩笑與垃圾話
- 業配與贊助商內容
- 與投資無關的科技或娛樂話題
- 重複內容
- 無資訊量的口語填充詞

請特別關注：
- 股票名稱與公司
- 產業趨勢（AI、半導體、雲端、能源等）
- 財報與法說會
- 營收、EPS、毛利率、估值
- 市場觀點與投資邏輯
- 總經（利率、Fed、通膨、景氣）
- 資金流向與市場情緒
- 投資風險與催化因素

輸出要求：
1. 使用繁體中文
2. 總長度限制在 300 字以內
3. 使用「主題式條列摘要」
4. 每點盡量精簡且高資訊密度

輸出格式：
【主題】
- 重點1
- 重點2

【另一主題】
- 重點1
- 重點2

以下是逐字稿：

{transcript}
"""


def _ollama_base_url() -> str:
    for host in ('localhost', 'host.docker.internal'):
        url = f'http://{host}:11434'
        try:
            requests.get(f'{url}/api/tags', timeout=2)
            return url
        except Exception:
            continue
    return 'http://localhost:11434'


def _summarize(transcript: str) -> str | None:
    base_url = _ollama_base_url()
    prompt = PROMPT_TEMPLATE.format(transcript=transcript)
    try:
        resp = requests.post(
            f'{base_url}/api/generate',
            json={
                'model': OLLAMA_MODEL,
                'prompt': prompt,
                'stream': False,
                'think': False,
                'options': {'temperature': 0.3, 'num_ctx': 32768},
            },
            timeout=300,
        )
        resp.raise_for_status()
        return resp.json().get('response', '').strip()
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


def _process_row(sheets, gooaye_sid: str, sheet_row: int, row: list, txt_path: Path) -> bool:
    """Summarize one episode, update sheet, notify GAS. Returns True on success."""
    title = row[COL_TITLE] if len(row) > COL_TITLE else txt_path.stem
    episode_id = row[0] if row else ''
    episode_url = row[COL_EPISODE_URL] if len(row) > COL_EPISODE_URL else ''

    print(f'\n  [{sheet_row}] {title[:70]}', flush=True)

    transcript = txt_path.read_text(encoding='utf-8').strip()
    if not transcript:
        print('    transcript file is empty, skipping')
        return False

    summary = _summarize(transcript)
    if not summary:
        return False

    print(f'    ✓ {len(summary)} chars')
    sheets_update_with_retry(
        sheets, gooaye_sid, f'Sheet1!K{sheet_row}',
        [[summary]],
    )
    _notify_gas(episode_id, title, summary, episode_url)
    return True


def main():
    print(f'[{datetime.now(UTC8)}] Starting Gooaye summarization...')

    gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    result = sheets.get(spreadsheetId=gooaye_sid, range='Sheet1!A2:K').execute()
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
        if _process_row(sheets, gooaye_sid, sheet_row, row, txt_path)
        or not time.sleep(2)  # sleep between episodes, always continue
    )

    print(f'\nDone. Summarized {summarized}/{len(to_summarize)} episode(s)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ep', help='Episode number (e.g. 662) — looks up row from sheet')
    args = parser.parse_args()

    gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    if args.ep:
        rows = sheets.get(spreadsheetId=gooaye_sid, range='Sheet1!A2:K').execute().get('values', [])
        tag = f'EP{args.ep}'
        match = next(
            ((i + 2, row) for i, row in enumerate(rows)
             if len(row) > COL_TITLE and tag in row[COL_TITLE]
             and len(row) > COL_LOCAL_TXT and row[COL_LOCAL_TXT].strip()),
            None,
        )
        if not match:
            print(f'No sheet row found for {tag} with a LocalTXT path')
            sys.exit(1)
        sheet_row, row = match
        txt_path = REPO_ROOT / row[COL_LOCAL_TXT].strip()
        if not txt_path.exists():
            print(f'TXT not found on disk: {txt_path}')
            sys.exit(1)
        _process_row(sheets, gooaye_sid, sheet_row, row, txt_path)
    else:
        main()
