"""Transcribe Gooaye 股癌 MP3 files using faster-whisper medium.

Cron: 0 10 * * * (daily at 10:00 UTC = 18:00 UTC+8, after download)
Reads rows where LocalMP3 is set and TranscribedAt is empty.
Saves transcript to <same-path>.txt (replacing .mp3 extension).
Updates TranscribedAt (col I) and LocalTXT (col J) in the sheet.

Strategy:
1. ffmpeg splits original MP3 into 60-second chunks in /tmp (stream copy, no re-encode)
2. Each chunk transcribed individually for real-time progress visibility
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
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
CHUNK_SECONDS = 60

# Column indices (0-based in values list)
COL_LOCAL_MP3 = 7
COL_TRANSCRIBED_AT = 8
COL_LOCAL_TXT = 9
COL_TITLE = 2


def _load_whisper():
    try:
        from faster_whisper import WhisperModel
    except ImportError:
        print("faster-whisper not installed. Run: pip install faster-whisper")
        sys.exit(1)
    print("Loading Whisper medium model (cuda/float16)...")
    return WhisperModel("medium", device="cuda", compute_type="float16")


def _split_mp3(mp3_path: Path, chunk_dir: Path) -> list[Path]:
    """Split mp3 into CHUNK_SECONDS-second pieces using ffmpeg (stream copy, no re-encode)."""
    pattern = str(chunk_dir / "chunk_%04d.mp3")
    subprocess.run(
        [
            "ffmpeg", "-y", "-i", str(mp3_path),
            "-f", "segment", "-segment_time", str(CHUNK_SECONDS),
            "-c", "copy", "-reset_timestamps", "1",
            pattern,
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )
    return sorted(chunk_dir.glob("chunk_*.mp3"))


_INITIAL_PROMPT = (
    "股癌 Gooaye，謝孟恭。美股、台股、投資、標的、主權基金、散戶、槓桿、對沖、做空、多頭、空頭、回測。"
    "台積電 TSMC、輝達 NVIDIA、微軟 Microsoft、蘋果 Apple、特斯拉 Tesla、超微 AMD、博通 Broadcom。"
    "聯發科、鴻海、廣達、緯創、世芯、創意。ETF、0050、0056、00878、00919、00929。"
    "本益比、殖利率、營收、毛利率、EPS、財報、法說會、資本支出、CoWoS、半導體、載板。"
    "被動元件、MLCC、電容、電感、產能、庫存、光通訊。"
    "加權指數、美債、殖利率倒掛、聯準會 Fed、降息、升息、通膨、籌碼面、技術面。"
    "融資、融券、回檔、修正、動能、倉位、太弱留強、波段操作。"
)

_FINANCIAL_KEYWORDS = [
    # 公司與產業
    "股", "台積", "TSMC", "輝達", "NVIDIA", "AMD", "Broadcom", "博通",
    "聯發科", "鴻海", "廣達", "緯創", "世芯", "創意",
    "半導體", "晶片", "CoWoS", "載板", "被動元件", "MLCC", "電容", "電感",
    "光通訊", "AI", "伺服器", "雲端",
    # 市場與指數
    "美股", "台股", "大盤", "加權", "指數", "OTC", "櫃買",
    "ETF", "0050", "00878", "高股息",
    # 基本面
    "營收", "毛利", "EPS", "財報", "法說會", "本益比", "殖利率",
    "資本支出", "產能", "庫存", "漲價", "毛利率", "ROE",
    # 總經
    "Fed", "聯準會", "降息", "升息", "通膨", "美債", "殖利率倒掛",
    "主權基金", "外資", "投信",
    # 交易策略
    "多頭", "空頭", "做空", "對沖", "槓桿", "融資", "融券",
    "回檔", "修正", "動能", "倉位", "波段", "加碼", "減碼",
    "籌碼", "技術面", "基本面", "太弱留強",
]


def _transcribe_episode(model, mp3_path: Path) -> str:
    chunk_dir = Path(tempfile.mkdtemp(prefix="gooaye_"))
    try:
        print(f"    Splitting into {CHUNK_SECONDS}s chunks...", flush=True)
        chunks = _split_mp3(mp3_path, chunk_dir)
        print(f"    {len(chunks)} chunks — transcribing:", flush=True)

        parts = []
        for i, chunk in enumerate(chunks, 1):
            if parts and any(kw in parts[-1] for kw in _FINANCIAL_KEYWORDS):
                prompt = _INITIAL_PROMPT + parts[-1][-100:]
            else:
                prompt = _INITIAL_PROMPT
            segments, _ = model.transcribe(
                str(chunk), language="zh", beam_size=5,
                temperature=0, initial_prompt=prompt,
                condition_on_previous_text=False,
            )
            text = " ".join(seg.text.strip() for seg in segments)
            parts.append(text)
            mins = (i - 1) * CHUNK_SECONDS // 60
            print(f"      [{i:3d}/{len(chunks)}] {mins:3d}m — {text[:80]}", flush=True)

        return "\n".join(parts)
    finally:
        shutil.rmtree(chunk_dir, ignore_errors=True)


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
        print(f"\n  [{sheet_row}] {mp3_path.name[:70]}", flush=True)

        if txt_path.exists():
            print(f"    transcript already exists, updating sheet")
        else:
            try:
                transcript = _transcribe_episode(model, mp3_path)
                txt_path.write_text(transcript, encoding='utf-8')
                kb = txt_path.stat().st_size / 1024
                print(f"    ✓ saved {kb:.0f} KB")
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--ep', help='Episode number (e.g. 662) — looks up MP3 path from sheet')
    args = parser.parse_args()

    if args.ep:
        gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
        sheets = get_podcast_sheets_service().spreadsheets().values()
        rows = sheets.get(spreadsheetId=gooaye_sid, range='Sheet1!A2:J').execute().get('values', [])

        tag = f"EP{args.ep}"
        match = next(
            (row for row in rows
             if len(row) > COL_TITLE and tag in row[COL_TITLE]
             and len(row) > COL_LOCAL_MP3 and row[COL_LOCAL_MP3].strip()),
            None,
        )
        if not match:
            print(f"No sheet row found for {tag} with a LocalMP3 path")
            sys.exit(1)

        mp3_path = REPO_ROOT / match[COL_LOCAL_MP3].strip()
        if not mp3_path.exists():
            print(f"MP3 not found on disk: {mp3_path}")
            sys.exit(1)

        print(f"Found: {match[COL_TITLE]}")
        print(f"MP3:   {mp3_path}")
        model = _load_whisper()
        transcript = _transcribe_episode(model, mp3_path)
        txt_path = mp3_path.with_suffix('.txt')
        txt_path.write_text(transcript, encoding='utf-8')
        kb = txt_path.stat().st_size / 1024
        print(f"\n✓ Saved {kb:.0f} KB → {txt_path}")
    else:
        main()
