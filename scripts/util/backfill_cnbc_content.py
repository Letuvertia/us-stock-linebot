"""Re-fetch content for CNBC rows that only contain boilerplate text."""
import sys
import os
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_news'))
import data_common
from news_common import fetch_article_content

CNBC_ID = '14yZCDkH7MCqb3YAiBp8yg5iJCkCYpWvlGmAvzoIx8uk'
BOILERPLATE = 'Got a confidential news tip'


def main():
    svc = common.get_sheets_service()
    sheets = svc.spreadsheets().values()

    result = sheets.get(spreadsheetId=CNBC_ID, range='Sheet1!A:G').execute()
    rows = result.get('values', [])
    header = rows[0]
    data = rows[1:]

    bad_rows = [
        (i + 2, row)  # 1-indexed sheet row
        for i, row in enumerate(data)
        if len(row) > 5 and BOILERPLATE in (row[4] if len(row) > 4 else '')
    ]
    print(f"Found {len(bad_rows)} rows to backfill out of {len(data)} total")

    updates = []
    for sheet_row, row in bad_rows:
        url = row[5] if len(row) > 5 else ''
        if not url:
            print(f"  Row {sheet_row}: no URL, skipping")
            continue

        content = fetch_article_content(url)
        if not content or BOILERPLATE in content:
            print(f"  Row {sheet_row}: still empty after re-fetch — {url.split('/')[-1][:50]}")
        else:
            print(f"  Row {sheet_row}: ✓ {len(content)} chars — {url.split('/')[-1][:50]}")
            updates.append({
                'range': f'Sheet1!E{sheet_row}',
                'values': [[content]],
            })

        time.sleep(random.uniform(1.5, 3.5))

    if updates:
        chunk_size = 20
        for i in range(0, len(updates), chunk_size):
            chunk = updates[i:i + chunk_size]
            body = {'valueInputOption': 'RAW', 'data': chunk}
            svc.spreadsheets().values().batchUpdate(spreadsheetId=CNBC_ID, body=body).execute()
            print(f"  Wrote rows {i+1}–{i+len(chunk)}/{len(updates)}")
            time.sleep(1.0)
        print(f"\nUpdated {len(updates)} rows.")
    else:
        print("\nNothing to update.")


if __name__ == '__main__':
    main()
