"""Fetch latest Gooaye 股癌 episodes from SoundOn RSS and store to Google Sheet.

Cron: 0 9 * * * (daily at 09:00 UTC = 17:00 UTC+8)
Deduplicates by AudioURL. Stores: ID, Date, Title, Duration, AudioURL, EpisodeURL.
"""
import os
import sys
import uuid
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'market_data'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from data_common import UTC8
from config import PODCAST_LOOKBACK_EPISODES
from podcast_common import (
    get_podcast_sheets_service,
    get_podcast_spreadsheet_id,
    load_existing_audio_urls,
    fetch_podcast_rss,
    append_with_retry,
)

GOOAYE_FEED_URL = 'https://feeds.soundon.fm/podcasts/954689a5-3096-43a4-a80b-7810b219cef3.xml'


def main():
    print(f"[{datetime.now(UTC8)}] Starting Gooaye RSS collection...")

    gooaye_sid = get_podcast_spreadsheet_id('Gooaye')
    sheets = get_podcast_sheets_service().spreadsheets().values()
    existing_urls = load_existing_audio_urls(sheets, gooaye_sid)
    print(f"Found {len(existing_urls)} existing episodes")

    episodes = fetch_podcast_rss(GOOAYE_FEED_URL)
    print(f"Fetched {len(episodes)} episodes from RSS")

    episodes = episodes[:PODCAST_LOOKBACK_EPISODES]

    new_rows = []
    for ep in episodes:
        if ep['audio_url'] in existing_urls:
            continue
        new_rows.append([
            str(uuid.uuid4()),
            ep['date'].strftime('%Y-%m-%d %H:%M:%S'),
            ep['title'],
            ep['duration'],
            ep['audio_url'],
            ep['link'],
            '',  # DownloadedAt
            '',  # LocalMP3
            '',  # TranscribedAt
            '',  # LocalTXT
        ])
        existing_urls.add(ep['audio_url'])

    if new_rows:
        # Reverse so oldest episode is appended first → sheet stays in ascending date order
        new_rows.reverse()
        append_with_retry(sheets, gooaye_sid, 'Sheet1!A:J', new_rows)
        print(f"Appended {len(new_rows)} new episode(s)")
    else:
        print("No new episodes")


if __name__ == '__main__':
    main()
