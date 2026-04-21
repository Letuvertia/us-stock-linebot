#!/usr/bin/env python3
"""Hourly RSS news collector. Fetches 9 feeds, tags tickers, writes to NewsStore sheet."""
import os
import re
import time
import uuid
import warnings
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta

UTC8 = timezone(timedelta(hours=8))

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

# --- Config ---
CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')
SNIPPET_LENGTH = 200

RSS_FEEDS = [
    ('CNBC Finance', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664', 'CNBC'),
    ('CNBC Investing', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069', 'CNBC'),
    ('CNBC Earnings', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135', 'CNBC'),
    ('CNBC Tech', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910', 'CNBC'),
    ('CNBC Economy', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258', 'CNBC'),
    ('CNBC Commentary', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100370673', 'CNBC'),
    ('Reuters Business', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+business&hl=en-US&gl=US&ceid=US:en', 'Reuters'),
    ('Reuters Markets', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+markets&hl=en-US&gl=US&ceid=US:en', 'Reuters'),
    ('Reuters Tech', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+technology&hl=en-US&gl=US&ceid=US:en', 'Reuters'),
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/xml, text/xml, application/rss+xml',
}


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def sheets_append_with_retry(sheets, range_, values, retries=5):
    for attempt in range(retries):
        try:
            sheets.append(
                spreadsheetId=SPREADSHEET_ID, range=range_,
                valueInputOption='RAW', insertDataOption='INSERT_ROWS',
                body={'values': values}
            ).execute()
            return
        except Exception as e:
            if attempt < retries - 1 and ('429' in str(e) or 'Timeout' in str(e) or 'timed out' in str(e)):
                wait = 30 * (attempt + 1)
                print(f"  Sheets error ({e.__class__.__name__}), retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def strip_html(text: str) -> str:
    return re.sub(r'<[^>]+>', '', text).strip()


def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len - 3] + '...'


def extract_ticker_tags(text: str, ticker_keywords: dict[str, list[str]]) -> list[str]:
    upper = text.upper()
    tags = []
    for ticker, keywords in ticker_keywords.items():
        for kw in keywords:
            if re.search(rf'\b{re.escape(kw.upper())}\b', upper):
                tags.append(ticker)
                break
    return tags


def fetch_rss_feed(name: str, url: str) -> list[dict]:
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
        description = item.findtext('description') or ''

        snippet = truncate(strip_html(description), SNIPPET_LENGTH)

        try:
            date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %Z') if pub_date else datetime.now(UTC8)
        except ValueError:
            try:
                date = datetime.strptime(pub_date, '%a, %d %b %Y %H:%M:%S %z').replace(tzinfo=None) if pub_date else datetime.now(UTC8)
            except ValueError:
                date = datetime.now(UTC8)

        if title and link:
            items.append({
                'title': title,
                'link': link,
                'date': date,
                'snippet': snippet,
            })

    return items


def main():
    print(f"[{datetime.now(UTC8)}] Starting RSS news collection...")
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    # 1. Read tickers (col A) and keywords (col AX) for tagging
    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:AX'
    ).execute()
    rows = result.get('values', [])
    ticker_keywords: dict[str, list[str]] = {}
    for row in rows:
        if not row:
            continue
        ticker = row[0]
        kw_col = 49  # AX = index 49 (0-based from A)
        keywords = []
        if len(row) > kw_col and row[kw_col].strip():
            keywords = [k.strip() for k in row[kw_col].split(',') if k.strip()]
        ticker_keywords[ticker] = keywords
    kw_count = sum(1 for kws in ticker_keywords.values() if kws)
    print(f"Loaded {len(ticker_keywords)} tickers for tagging ({kw_count} with keywords)")

    # 2. Read existing URLs for dedup
    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='NewsStore!F2:F'
    ).execute()
    existing_urls = set(row[0] for row in result.get('values', []) if row)
    print(f"Found {len(existing_urls)} existing articles")

    # 3. Fetch all RSS feeds
    all_new = []
    now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

    for feed_name, feed_url, source in RSS_FEEDS:
        print(f"  Fetching {feed_name}...", end=' ')
        items = fetch_rss_feed(feed_name, feed_url)

        new_count = 0
        for item in items:
            if item['link'] in existing_urls:
                continue

            tags = extract_ticker_tags(item['title'] + ' ' + item['snippet'], ticker_keywords)
            date_str = item['date'].strftime('%Y-%m-%d %H:%M:%S')

            all_new.append([
                str(uuid.uuid4()),
                date_str,
                ','.join(tags),
                item['title'],
                item['snippet'],
                item['link'],
                now,
            ])
            existing_urls.add(item['link'])
            new_count += 1

        print(f"{len(items)} items, {new_count} new")
        time.sleep(0.5)

    # 4. Batch append to NewsStore
    if all_new:
        sheets_append_with_retry(sheets, 'NewsStore!A:G', all_new)
        print(f"\nAppended {len(all_new)} new articles to NewsStore")
    else:
        print("\nNo new articles to add")

    print(f"[{datetime.now(UTC8)}] Done!")


if __name__ == '__main__':
    main()
