"""Shared utilities for news collection scripts (CNBC + Reuters)."""
import http.cookiejar
import os
import random
import re
import time
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

from common import UTC8, _is_retryable, get_news_sheet_ids, get_sheets_service

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
NEWS_CREDS_FILE = os.environ.get('NEWS_CREDS_FILE', os.path.join(
    _REPO_ROOT, 'juns-stock-agent-f58e7f8b7eba.json',
))
USER_CONFIG_SPREADSHEET_ID = os.environ.get(
    'USER_CONFIG_SPREADSHEET_ID', '1rIVv2lZDrUT7bCO8iXzl5g5J_-BKA7RjusT64akZD0k'
)

_news_sheet_ids_cache = None

def get_news_spreadsheet_id(source: str) -> str:
    """Look up a news source spreadsheet ID from NewsSheetIDs tab."""
    global _news_sheet_ids_cache
    if _news_sheet_ids_cache is None:
        _news_sheet_ids_cache = get_news_sheet_ids()
    sid = _news_sheet_ids_cache.get(source)
    if not sid:
        raise ValueError(f"No spreadsheet ID found for news source '{source}' in NewsSheetIDs tab")
    return sid

_USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:138.0) Gecko/20100101 Firefox/138.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.4 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
]

_ACCEPT_LANGUAGES = [
    'en-US,en;q=0.9',
    'en-US,en;q=0.9,zh-TW;q=0.8',
    'en,en-US;q=0.9',
]

_cookie_jar = http.cookiejar.CookieJar()
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(_cookie_jar))


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _browser_headers(accept: str = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8') -> dict:
    return {
        'User-Agent': _random_ua(),
        'Accept': accept,
        'Accept-Language': random.choice(_ACCEPT_LANGUAGES),
        'Accept-Encoding': 'identity',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }


def _human_delay(lo: float = 1.5, hi: float = 4.0):
    time.sleep(random.uniform(lo, hi))


BOILERPLATE_PATTERNS = re.compile(
    r'sign up for|delivered to your inbox|get more cnbc|all rights reserved|data is a real-time|'
    r'data also provided|disclaimer|©|privacy policy|terms of service|'
    r'confidential news tip|we want to hear from you|subscribe to cnbc|'
    r'read more\s*subscribe|licensing & reprints|cnbc councils|cnbc panel|'
    r'closed captioning|corrections\s*about|site map\s*careers|'
    r'news releases|digital products|join the cnbc|about cnbc|'
    r'subscribe to investing club|got a confidential|'
    r'sign up for our weekly newsletter|sign up for our daily newsletter',
    re.IGNORECASE,
)


def get_news_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        NEWS_CREDS_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets'],
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def load_ticker_keywords() -> dict[str, list[str]]:
    """Load ticker keywords from user-config sheet (uses stock service account)."""
    stock_sheets = get_sheets_service().spreadsheets().values()
    result = stock_sheets.get(
        spreadsheetId=USER_CONFIG_SPREADSHEET_ID,
        range="'News Keywords'!A2:H"
    ).execute()
    rows = result.get('values', [])
    ticker_keywords: dict[str, list[str]] = {}
    for row in rows:
        if not row:
            continue
        ticker = row[0]
        keywords = []
        if len(row) > 7 and row[7].strip():
            keywords = [k.strip() for k in row[7].split(',') if k.strip()]
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


def _clean_paragraphs(raw_texts: list[str]) -> list[str]:
    cleaned = []
    for t in raw_texts:
        t = strip_html(t)
        if len(t) < 40 or BOILERPLATE_PATTERNS.search(t):
            continue
        if t.count(' ') < len(t) * 0.08:
            continue
        cleaned.append(t)
    return cleaned


def _extract_paragraphs(html_text: str) -> list[str]:
    """Extract article text from HTML, trying article body first, then all <p> tags."""
    article_match = re.search(
        r'class="ArticleBody-articleBody"[^>]*>(.*)',
        html_text, re.DOTALL | re.IGNORECASE,
    )

    if article_match:
        body = article_match.group(1)
        xyz = re.search(r'class="xyz-data"[^>]*>(.*?)</span>', body, re.DOTALL)
        if xyz:
            raw_text = strip_html(xyz.group(1))
            if len(raw_text) > 200:
                sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', raw_text)
                return _clean_paragraphs(sentences)

        paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', body, re.DOTALL | re.IGNORECASE)
        if paragraphs:
            return _clean_paragraphs(paragraphs)

    paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', html_text, re.DOTALL | re.IGNORECASE)
    return _clean_paragraphs(paragraphs)


def fetch_article_content(url: str) -> str:
    try:
        headers = _browser_headers()
        headers['Referer'] = 'https://www.google.com/'
        req = urllib.request.Request(url, headers=headers)
        with _opener.open(req, timeout=15) as resp:
            if resp.status != 200:
                return ''
            html_text = resp.read().decode('utf-8', errors='replace')
    except Exception:
        return ''

    return '\n'.join(_extract_paragraphs(html_text))


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
        headers = _browser_headers(accept='application/xml, text/xml, application/rss+xml, */*')
        req = urllib.request.Request(url, headers=headers)
        with _opener.open(req, timeout=15) as resp:
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
                      values: list, retries: int = 5, batch_size: int = 10):
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
