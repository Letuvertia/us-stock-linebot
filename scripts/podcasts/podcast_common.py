"""Shared utilities for podcast collection scripts."""
import os
import random
import re
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from html.parser import HTMLParser

from google.oauth2 import service_account
from googleapiclient.discovery import build

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8, US_STOCK_SPREADSHEET_ID, _is_retryable
from config import NEWS_CREDS_FILE

_ITUNES_NS = 'http://www.itunes.com/dtds/podcast-1.0.dtd'


class _TagStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._parts = []

    def handle_data(self, data):
        self._parts.append(data)

    def get_text(self):
        return ''.join(self._parts)


def _strip_html(html: str) -> str:
    p = _TagStripper()
    p.feed(html)
    return p.get_text()


def _description_headline(description: str) -> str:
    """Return the first non-empty, non-sponsor line from a description."""
    text = _strip_html(description)
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ''

_podcast_sheet_ids_cache: dict[str, str] | None = None


def get_podcast_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        NEWS_CREDS_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def get_podcast_spreadsheet_id(source: str) -> str:
    """Look up podcast spreadsheet ID from PodcastSheetIDs tab."""
    global _podcast_sheet_ids_cache
    if _podcast_sheet_ids_cache is None:
        from data_common import get_sheets_service
        sheets = get_sheets_service().spreadsheets().values()
        result = sheets.get(
            spreadsheetId=US_STOCK_SPREADSHEET_ID,
            range='PodcastSheetIDs!A2:B',
        ).execute()
        _podcast_sheet_ids_cache = {
            r[0]: r[1] for r in result.get('values', []) if len(r) >= 2
        }
    sid = _podcast_sheet_ids_cache.get(source)
    if not sid:
        raise ValueError(f"No spreadsheet ID found for podcast source '{source}' in PodcastSheetIDs tab")
    return sid


def load_existing_audio_urls(sheets_values, spreadsheet_id: str) -> set[str]:
    """Return set of AudioURLs already in the sheet (column E)."""
    result = sheets_values.get(
        spreadsheetId=spreadsheet_id, range='Sheet1!E2:E'
    ).execute()
    return {row[0] for row in result.get('values', []) if row}


def _parse_duration(duration_str: str) -> int:
    """Convert itunes:duration to seconds. Accepts '3600', '60:00', or '1:00:00'."""
    if not duration_str:
        return 0
    parts = duration_str.strip().split(':')
    try:
        if len(parts) == 1:
            return int(parts[0])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except ValueError:
        pass
    return 0


def fetch_podcast_rss(feed_url: str) -> list[dict]:
    """Fetch and parse a podcast RSS feed. Returns list of episode dicts."""
    try:
        req = urllib.request.Request(feed_url, headers={'User-Agent': 'python-podcast-collector/1.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            xml_bytes = resp.read()
    except Exception as e:
        print(f"  RSS fetch error: {e}")
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  RSS parse error: {e}")
        return []

    channel = root.find('channel')
    if channel is None:
        return []

    episodes = []
    for item in channel.findall('item'):
        rss_title = (item.findtext('title') or '').strip()
        link = (item.findtext('link') or '').strip()
        pub_date = item.findtext('pubDate') or ''

        enclosure = item.find('enclosure')
        audio_url = enclosure.get('url', '').strip() if enclosure is not None else ''

        duration_el = item.find(f'{{{_ITUNES_NS}}}duration')
        duration = _parse_duration((duration_el.text or '').strip() if duration_el is not None else '')

        if not audio_url:
            continue

        # Combine short RSS title (e.g. "EP662 | ⛑️") with Chinese headline from description
        headline = _description_headline(item.findtext('description') or '')
        title = f"{rss_title} — {headline}" if headline else rss_title

        # Normalize timezone abbreviations (GMT, UTC) to offset form so %z always works
        pub_date_norm = pub_date.replace(' GMT', ' +0000').replace(' UTC', ' +0000')
        try:
            date = datetime.strptime(pub_date_norm, '%a, %d %b %Y %H:%M:%S %z').astimezone(UTC8).replace(tzinfo=None)
        except ValueError:
            date = datetime.now(UTC8).replace(tzinfo=None)

        episodes.append({
            'title': title,
            'link': link,
            'audio_url': audio_url,
            'duration': duration,
            'date': date,
        })

    return episodes


def append_with_retry(sheets_values, spreadsheet_id: str, range_: str,
                      values: list, retries: int = 5, batch_size: int = 10) -> None:
    for i in range(0, len(values), batch_size):
        chunk = values[i:i + batch_size]
        for attempt in range(retries):
            try:
                sheets_values.append(
                    spreadsheetId=spreadsheet_id, range=range_,
                    valueInputOption='RAW', insertDataOption='INSERT_ROWS',
                    body={'values': chunk},
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


def sheets_update_with_retry(sheets_values, spreadsheet_id: str, range_: str,
                             values: list, retries: int = 3) -> None:
    for attempt in range(retries):
        try:
            sheets_values.update(
                spreadsheetId=spreadsheet_id, range=range_,
                valueInputOption='RAW', body={'values': values},
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and _is_retryable(e):
                wait = 30 * (attempt + 1)
                print(f"  Sheets update error ({type(e).__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise
