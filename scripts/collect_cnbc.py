#!/usr/bin/env python3
"""Hourly CNBC RSS news collector. Fetches 6 CNBC feeds, tags tickers, writes to CNBC News sheet."""
import os
import sys
import time
import uuid
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import UTC8
from news_common import (
    fetch_rss_feed, fetch_article_content, extract_ticker_tags,
    load_ticker_keywords, load_existing_urls, append_with_retry,
    get_news_sheets_service, get_news_spreadsheet_id,
)

CNBC_FEEDS = [
    ('CNBC Finance', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664'),
    ('CNBC Investing', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069'),
    ('CNBC Earnings', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839135'),
    ('CNBC Tech', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=19854910'),
    ('CNBC Economy', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258'),
    ('CNBC Commentary', 'https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100370673'),
]


def main():
    print(f"[{datetime.now(UTC8)}] Starting CNBC news collection...")
    cnbc_sid = get_news_spreadsheet_id('CNBC')
    sheets = get_news_sheets_service().spreadsheets().values()

    ticker_keywords = load_ticker_keywords()
    existing_urls = load_existing_urls(sheets, cnbc_sid)
    print(f"Found {len(existing_urls)} existing CNBC articles")

    new_rows = []
    now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

    for feed_name, feed_url in CNBC_FEEDS:
        print(f"  Fetching {feed_name}...", end=' ')
        items = fetch_rss_feed(feed_name, feed_url)

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
        append_with_retry(sheets, cnbc_sid, 'Sheet1!A:G', new_rows)
        print(f"\nAppended {len(new_rows)} new articles to CNBC News")
    else:
        print("\nNo new CNBC articles")

    print(f"[{datetime.now(UTC8)}] Done!")


if __name__ == '__main__':
    main()
