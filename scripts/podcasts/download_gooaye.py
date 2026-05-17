"""Download Gooaye 股癌 MP3 files for episodes not yet downloaded.

Cron: 30 9 * * * (daily at 09:30 UTC = 17:30 UTC+8, after collect)
Saves to podcasts_data/gooaye/<date>-<slug>.mp3 (repo root relative).
Updates DownloadedAt (col G) and LocalMP3 (col H) in the sheet.
"""
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8
from podcast_common import (
    get_podcast_sheets_service,
    get_podcast_spreadsheet_id,
    sheets_update_with_retry,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_DIR = REPO_ROOT / 'podcasts_data' / 'gooaye'

# Column indices (0-based in values list)
COL_DATE = 1
COL_TITLE = 2
COL_AUDIO_URL = 4
COL_DOWNLOADED_AT = 6
COL_LOCAL_MP3 = 7


def _slug(title: str) -> str:
    return re.sub(r'[^\w一-鿿-]', '-', title)[:60].strip('-')


def main():
    print(f"[{datetime.now(UTC8)}] Starting Gooaye MP3 download...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    result = sheets.get(spreadsheetId=gooaye_sid, range='Sheet1!A2:J').execute()
    rows = result.get('values', [])
    if not rows:
        print("No episodes in sheet")
        return

    to_download = [
        (i + 2, row) for i, row in enumerate(rows)
        if len(row) > COL_AUDIO_URL
        and row[COL_AUDIO_URL].strip()
        and (len(row) <= COL_DOWNLOADED_AT or not row[COL_DOWNLOADED_AT].strip())
    ]
    print(f"Found {len(to_download)} episode(s) to download")

    downloaded = 0
    for sheet_row, row in to_download:
        date_str = (row[COL_DATE] if len(row) > COL_DATE else 'unknown')[:10]
        title = row[COL_TITLE] if len(row) > COL_TITLE else 'untitled'
        audio_url = row[COL_AUDIO_URL]

        filename = f"{date_str}-{_slug(title)}.mp3"
        local_path = OUTPUT_DIR / filename
        rel_path = str(local_path.relative_to(REPO_ROOT))

        print(f"  [{sheet_row}] {filename[:70]}")

        if local_path.exists():
            print(f"    already exists, updating sheet")
        else:
            try:
                resp = requests.get(audio_url, stream=True, timeout=60,
                                    headers={'User-Agent': 'python-podcast-downloader/1.0'})
                resp.raise_for_status()
                total = int(resp.headers.get('content-length', 0))
                downloaded_bytes = 0
                with open(local_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 256):
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                mb = downloaded_bytes / 1024 / 1024
                print(f"    ✓ {mb:.1f} MB")
            except Exception as e:
                print(f"    ✗ download failed: {e}")
                if local_path.exists():
                    local_path.unlink()
                continue

        now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')
        sheets_update_with_retry(
            sheets, gooaye_sid, f'Sheet1!G{sheet_row}:H{sheet_row}',
            [[now, rel_path]],
        )
        downloaded += 1

    print(f"\nDone. Downloaded {downloaded}/{len(to_download)} episode(s)")


if __name__ == '__main__':
    main()
