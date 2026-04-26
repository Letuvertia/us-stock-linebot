#!/usr/bin/env python3
"""Hourly Reuters RSS news collector. Fetches 3 Reuters feeds via Google News, tags tickers, writes to Reuters News sheet."""
import os
import sys
import re
import time
import uuid
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import UTC8, _is_retryable
from news_common import (
    fetch_rss_feed, fetch_article_content, extract_ticker_tags,
    load_ticker_keywords, load_existing_urls, append_with_retry,
    get_news_sheets_service, decode_google_news_url,
)

REUTERS_NEWS_SPREADSHEET_ID = os.environ.get('REUTERS_NEWS_SPREADSHEET_ID', '1Hr-5CEzZjKTh2_1xX3oclX6ngSj00EZF_6C2t7QDpqg')

REUTERS_FEEDS = [
    ('Reuters Business', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+business&hl=en-US&gl=US&ceid=US:en'),
    ('Reuters Markets', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+markets&hl=en-US&gl=US&ceid=US:en'),
    ('Reuters Tech', 'https://news.google.com/rss/search?q=when:24h+allinurl:reuters.com+technology&hl=en-US&gl=US&ceid=US:en'),
]


def main():
    print(f"[{datetime.now(UTC8)}] Starting Reuters news collection...")
    sheets = get_news_sheets_service().spreadsheets().values()

    ticker_keywords = load_ticker_keywords(sheets)
    existing_urls = load_existing_urls(sheets, REUTERS_NEWS_SPREADSHEET_ID)
    print(f"Found {len(existing_urls)} existing Reuters articles")

    new_rows = []
    now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

    for feed_name, feed_url in REUTERS_FEEDS:
        print(f"  Fetching {feed_name}...", end=' ')
        items = fetch_rss_feed(feed_name, feed_url, decode_google_urls=True)

        new_count = 0
        for item in items:
            if item['link'] in existing_urls:
                continue

            content = fetch_article_content(item['link'])
            time.sleep(1.0)

            tags = extract_ticker_tags(item['title'] + ' ' + content, ticker_keywords)
            date_str = item['date'].strftime('%Y-%m-%d %H:%M:%S')

            new_rows.append([
                str(uuid.uuid4()),
                date_str,
                ','.join(tags),
                item['title'],
                content,
                item['link'],
                now,
            ])
            existing_urls.add(item['link'])
            new_count += 1

        print(f"{len(items)} items, {new_count} new")
        time.sleep(0.5)

    if new_rows:
        append_with_retry(sheets, REUTERS_NEWS_SPREADSHEET_ID, 'Sheet1!A:G', new_rows)
        print(f"\nAppended {len(new_rows)} new articles to Reuters News")
    else:
        print("\nNo new Reuters articles")

    print(f"[{datetime.now(UTC8)}] Done!")


if __name__ == '__main__':
    main()
