"""Transcribe Gooaye 股癌 MP3 files using faster-whisper large-v3.

Cron: 0 10 * * * (daily at 10:00 UTC = 18:00 UTC+8, after download)
Reads rows where LocalMP3 is set and TranscribedAt is empty.
Saves transcript to <same-path>.txt (replacing .mp3 extension).
Updates TranscribedAt (col I) and LocalTXT (col J) in the sheet.
"""
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8
from podcast_common import (
    get_podcast_sheets_service,
    get_podcast_spreadsheet_id,
    sheets_update_with_retry,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

# Column indices (0-based in values list)
COL_LOCAL_MP3 = 7
COL_TRANSCRIBED_AT = 8
COL_LOCAL_TXT = 9


def _load_whisper():
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        print("faster-whisper not installed. Run: pip install faster-whisper")
        sys.exit(1)
    print("Loading Whisper large-v3 model (cpu/int8)...")
    return WhisperModel("large-v3", device="cpu", compute_type="int8")


def main():
    print(f"[{datetime.now(UTC8)}] Starting Gooaye transcription...")

    gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
    sheets = get_podcast_sheets_service().spreadsheets().values()

    result = sheets.get(spreadsheetId=gooaye_sid, range='Sheet1!A2:J').execute()
    rows = result.get('values', [])
    if not rows:
        print("No episodes in sheet")
        return

    to_transcribe = []
    for i, row in enumerate(rows):
        if len(row) <= COL_LOCAL_MP3 or not row[COL_LOCAL_MP3].strip():
            continue
        if len(row) > COL_TRANSCRIBED_AT and row[COL_TRANSCRIBED_AT].strip():
            continue
        mp3_rel = row[COL_LOCAL_MP3].strip()
        mp3_path = REPO_ROOT / mp3_rel
        if not mp3_path.exists():
            print(f"  [row {i+2}] MP3 not found on disk: {mp3_rel}, skipping")
            continue
        to_transcribe.append((i + 2, row, mp3_path))

    print(f"Found {len(to_transcribe)} episode(s) to transcribe")
    if not to_transcribe:
        return

    model = _load_whisper()

    transcribed = 0
    for sheet_row, row, mp3_path in to_transcribe:
        txt_path = mp3_path.with_suffix('.txt')
        rel_txt = str(txt_path.relative_to(REPO_ROOT))
        print(f"  [{sheet_row}] {mp3_path.name[:70]}")

        if txt_path.exists():
            print(f"    transcript already exists, updating sheet")
        else:
            try:
                segments, info = model.transcribe(
                    str(mp3_path),
                    language="zh",
                    beam_size=5,
                )
                transcript = "\n".join(seg.text.strip() for seg in segments)
                txt_path.write_text(transcript, encoding='utf-8')
                kb = txt_path.stat().st_size / 1024
                print(f"    ✓ {kb:.0f} KB ({info.duration:.0f}s audio, lang={info.language})")
            except Exception as e:
                print(f"    ✗ transcription failed: {e}")
                if txt_path.exists():
                    txt_path.unlink()
                continue

        now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')
        sheets_update_with_retry(
            sheets, gooaye_sid, f'Sheet1!I{sheet_row}:J{sheet_row}',
            [[now, rel_txt]],
        )
        transcribed += 1

    print(f"\nDone. Transcribed {transcribed}/{len(to_transcribe)} episode(s)")


if __name__ == '__main__':
    main()
