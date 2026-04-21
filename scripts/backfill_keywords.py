#!/usr/bin/env python3
"""One-time backfill: populate Keywords column (AX) from Name column (C).
Strips common corporate suffixes to extract a meaningful keyword.
Skips rows that already have keywords.
"""
import os
import re
import time
import warnings

warnings.filterwarnings("ignore")

from google.oauth2 import service_account
from googleapiclient.discovery import build

CREDS_FILE = os.environ.get('GOOGLE_CREDS_FILE', '/mnt/c/Users/1026o/Desktop/us-stock-linebot/juns-stock-agent-5f32b75f7c83.json')
SPREADSHEET_ID = os.environ.get('SPREADSHEET_ID', '1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU')

SUFFIXES = [
    r'\bInc\.?$', r'\bCorp\.?$', r'\bCorporation$', r'\bCompany$', r'\bCo\.?$',
    r'\bLtd\.?$', r'\bLimited$', r'\bPLC$', r'\bN\.?V\.?$', r'\bS\.?A\.?$',
    r'\bSE$', r'\bAG$', r'\bGroup$', r'\bHoldings$', r'\bHolding$',
    r'\bTechnologies$', r'\bTechnology$', r'\bPlatforms$', r'\bPlatform$',
    r'\bSolutions$', r'\bSystems$', r'\bEnterprises$', r'\bEnterprise$',
    r'\bIndustries$', r'\bInternational$', r'\bGlobal$',
    r'\bPharmaceuticals$', r'\bTherapeutics$', r'\bBiosciences$',
    r'\bBancorp$', r'\bFinancial$', r'\bBancshares$',
    r'\bClass [A-C]$', r'\bCl [A-C]$',
    r',\s*$',
]

SUFFIX_RE = re.compile('|'.join(SUFFIXES), re.IGNORECASE)


def extract_keyword(name: str) -> str:
    if not name:
        return ''
    result = name.strip()
    for _ in range(5):
        cleaned = SUFFIX_RE.sub('', result).strip().rstrip(',').strip()
        if cleaned == result:
            break
        result = cleaned
    result = result.rstrip('&').strip()
    if result.endswith('.com'):
        result = result[:-4]
    if len(result) <= 1:
        return ''
    return result


def get_sheets_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_FILE, scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    return build('sheets', 'v4', credentials=creds, cache_discovery=False)


def main():
    service = get_sheets_service()
    sheets = service.spreadsheets().values()

    result = sheets.get(
        spreadsheetId=SPREADSHEET_ID,
        range='StockUniverse!A2:AX'
    ).execute()
    rows = result.get('values', [])
    print(f"Read {len(rows)} rows")

    updates = []
    for i, row in enumerate(rows):
        ticker = row[0] if row else ''
        name = row[2] if len(row) > 2 else ''
        existing_kw = row[49] if len(row) > 49 else ''

        if not ticker or not name:
            continue
        if existing_kw.strip():
            continue

        keyword = extract_keyword(name)
        if not keyword:
            continue

        row_num = i + 2
        updates.append((row_num, keyword, ticker, name))

    print(f"Will populate {len(updates)} keyword entries")

    for ticker, name, keyword, _ in updates[:10]:
        print(f"  {ticker}: '{name}' -> '{keyword}'")
    if len(updates) > 10:
        print(f"  ... and {len(updates) - 10} more")

    # Batch update using batchUpdate to stay within quota
    batch_data = []
    for row_num, keyword, ticker, name in updates:
        batch_data.append({
            'range': f'StockUniverse!AX{row_num}',
            'values': [[keyword]]
        })

    CHUNK = 500
    for start in range(0, len(batch_data), CHUNK):
        chunk = batch_data[start:start + CHUNK]
        sheets.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={'valueInputOption': 'RAW', 'data': chunk}
        ).execute()
        print(f"  Wrote rows {start+1}-{start+len(chunk)}")

    print(f"\nDone! Populated {len(updates)} keywords.")


if __name__ == '__main__':
    main()
