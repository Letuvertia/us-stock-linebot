"""Shared utilities for news collection scripts (CNBC + Reuters)."""
import os
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

from common import UTC8, _is_retryable

NEWS_CREDS_FILE = os.environ.get('NEWS_CREDS_FILE', os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'juns-stock-agent-f58e7f8b7eba.json',
))
USER_CONFIG_SPREADSHEET_ID = os.environ.get(
    'USER_CONFIG_SPREADSHEET_ID', '1rIVv2lZDrUT7bCO8iXzl5g5J_-BKA7RjusT64akZD0k'
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/xml, text/xml, application/rss+xml',
}

BOILERPLATE_PATTERNS = re.compile(
    r'sign up for|delivered to your inbox|get more cnbc|all rights reserved|data is a real-time|'
    r'data also provided|disclaimer|©|privacy policy|terms of service',
    re.IGNORECASE,
)


def get_news_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        NEWS_CREDS_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def load_ticker_keywords(sheets_values) -> dict[str, list[str]]:
    result = sheets_values.get(
        spreadsheetId=USER_CONFIG_SPREADSHEET_ID,
        range="'News Keywords'!A2:D"
    ).execute()
    rows = result.get('values', [])
    ticker_keywords: dict[str, list[str]] = {}
    for row in rows:
        if not row:
            continue
        ticker = row[0]
        keywords = []
        if len(row) > 3 and row[3].strip():
            keywords = [k.strip() for k in row[3].split(',') if k.strip()]
        ticker_keywords[ticker] = keywords
    kw_count = sum(1 for kws in ticker_keywords.values() if kws)
    print(f"Loaded {len(ticker_keywords)} tickers for tagging ({kw_count} with keywords)")
    return ticker_keywords


def load_existing_urls(sheets_values, spreadsheet_id: str) -> set[str]:
    result = sheets_values.get(
        spreadsheetId=spreadsheet_id, range='Sheet1!F2:F'
    ).execute()
    urls = {row[0] for row in result.get('values', []) if row}
    return urls


def strip_html(text: str) -> str:
    import html
    return html.unescape(re.sub(r'<[^>]+>', '', text)).strip()


def fetch_article_content(url: str) -> str:
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': HEADERS['User-Agent'],
            'Accept': 'text/html,application/xhtml+xml',
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status != 200:
                return ''
            html_text = resp.read().decode('utf-8', errors='replace')
    except Exception:
        return ''

    paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_text, re.DOTALL | re.IGNORECASE)
    cleaned = []
    for p in paragraphs:
        t = strip_html(p)
        if len(t) < 40 or BOILERPLATE_PATTERNS.search(t):
            continue
        if t.count(' ') < len(t) * 0.08:
            continue
        cleaned.append(t)
    return '\n'.join(cleaned)


def extract_ticker_tags(text: str, ticker_keywords: dict[str, list[str]]) -> list[str]:
    upper = text.upper()
    tags = []
    for ticker, keywords in ticker_keywords.items():
        for kw in keywords:
            if re.search(rf'\b{re.escape(kw.upper())}\b', upper):
                tags.append(ticker)
                break
    return tags


def decode_google_news_url(url: str) -> str:
    try:
        from googlenewsdecoder import gnewsdecoder
        result = gnewsdecoder(url, interval=1)
        if result.get('status'):
            return result['decoded_url']
    except Exception:
        pass
    return url


def fetch_rss_feed(name: str, url: str, decode_google_urls: bool = False) -> list[dict]:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status != 200:
                print(f"  HTTP {resp.status}")
                return []
            xml_text = resp.read().decode()
    except Exception as e:
        print(f"  fetch error: {e}")
        return []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}")
        return []

    channel = root.find('channel')
    if channel is None:
        return []

    items = []
    for item in channel.findall('item'):
        title = (item.findtext('title') or '').strip()
        link = (item.findtext('link') or '').strip()
        pub_date = item.findtext('pubDate') or ''

        try:
            date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %Z') if pub_date else datetime.now(UTC8)
        except ValueError:
            try:
                date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=None) if pub_date else datetime.now(UTC8)
            except ValueError:
                date = datetime.now(UTC8)

        if title and link:
            if decode_google_urls and 'news.google.com' in link:
                link = decode_google_news_url(link)
            items.append({
                'title': title,
                'link': link,
                'date': date,
            })

    return items


def append_with_retry(sheets_values, spreadsheet_id: str, range_: str,
                      values: list, retries: int = 5, batch_size: int = 50):
    for i in range(0, len(values), batch_size):
        chunk = values[i:i + batch_size]
        for attempt in range(retries):
            try:
                sheets_values.append(
                    spreadsheetId=spreadsheet_id, range=range_,
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
