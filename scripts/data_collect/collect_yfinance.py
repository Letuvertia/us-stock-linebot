#!/usr/bin/env python3
"""Daily yfinance collector. OHLCV + fundamentals → individual stock sheets."""
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, get_sheets_service, get_stock_sheet_ids, get_header_map,
    find_or_create_today_row, write_stock_data, read_existing_row, round_if,
    get_trading_date, get_universe_ticker_rows, get_universe_header_map,
    batch_write_universe,
)

WRITE_INTERVAL = 3.0
UNIVERSE_BATCH_SIZE = 20


def fetch_ticker_data(ticker: str) -> dict:
    """Fetch yfinance data for a ticker, return column_name→value dict."""
    import yfinance as yf

    t = yf.Ticker(ticker)

    # OHLCV — most recent trading day
    hist = t.history(period='5d')
    ohlcv = {}
    if not hist.empty:
        last = hist.iloc[-1]
        ohlcv = {
            'Open': round_if(last.get('Open'), 2),
            'High': round_if(last.get('High'), 2),
            'Low': round_if(last.get('Low'), 2),
            'Close': round_if(last.get('Close'), 2),
            'Volume': int(last.get('Volume', 0)) if last.get('Volume') else '',
        }

    # Last known dividend and stock split (from full history)
    try:
        divs = t.dividends
        if not divs.empty:
            ohlcv['Dividends'] = round_if(divs.iloc[-1], 4)
    except Exception:
        pass
    try:
        splits = t.splits
        non_zero = splits[splits > 0]
        if not non_zero.empty:
            ohlcv['Stock_Splits'] = non_zero.iloc[-1]
    except Exception:
        pass

    # Fundamentals from .info
    try:
        info = t.info
    except Exception:
        info = {}

    def g(key, digits=None):
        v = info.get(key, '')
        if v is None:
            return ''
        if digits is not None and v != '':
            return round_if(v, digits)
        return v

    def epoch_to_date(key):
        v = info.get(key)
        if not v:
            return ''
        try:
            return datetime.fromtimestamp(v).strftime('%Y-%m-%d')
        except Exception:
            return ''

    data = {
        **ohlcv,
        # Range
        'All_Time_High': round_if(info.get('allTimeHigh'), 2),
        'All_Time_Low': round_if(info.get('allTimeLow'), 4),
        # Technical
        '50D_MA': round_if(info.get('fiftyDayAverage'), 2),
        '200D_MA': round_if(info.get('twoHundredDayAverage'), 2),
        '50D_MA_Change_Pct': round_if(info.get('fiftyDayAverageChangePercent', ''), 4),
        '200D_MA_Change_Pct': round_if(info.get('twoHundredDayAverageChangePercent', ''), 4),
        # Adj close — yfinance doesn't provide this directly in .history() anymore;
        # use close as proxy (splits/dividends already adjusted in yfinance v2)
        'Adj_Close': round_if(ohlcv.get('Close'), 2),
        # Earnings
        'Earnings_Growth': round_if(info.get('earningsGrowth'), 4),
        'EBITDA': g('ebitda'),
        'EBITDA_Margin': round_if(info.get('ebitdaMargins'), 4),
        'Gross_Profit': g('grossProfits'),
        'Revenue_Total': g('totalRevenue'),
        # Cash
        'Total_Cash': g('totalCash'),
        'Total_Debt': g('totalDebt'),
        'Operating_Cash_Flow': g('operatingCashflow'),
        'Free_Cash_Flow': g('freeCashflow'),
        # Dividends
        'Dividend_Rate': round_if(info.get('dividendRate'), 4),
        '5Y_Avg_Dividend_Yield': round_if(info.get('fiveYearAvgDividendYield'), 2),
        'Ex_Dividend_Date': epoch_to_date('exDividendDate'),
        # Shares
        'Shares_Outstanding': g('sharesOutstanding'),
        'Float_Shares': g('floatShares'),
        'Full_Time_Employees': g('fullTimeEmployees'),
        # Short interest
        'Shares_Short': g('sharesShort'),
        'Short_Ratio': round_if(info.get('shortRatio'), 2),
        'Short_Pct_Float': round_if(info.get('shortPercentOfFloat'), 4),
        # Ownership
        'Insider_Holdings_Pct': round_if(info.get('heldPercentInsiders'), 4),
        'Institutional_Holdings_Pct': round_if(info.get('heldPercentInstitutions'), 4),
        # Info
        'Sector': info.get('sector', ''),
        # Governance
        'Audit_Risk': g('auditRisk'),
        'Board_Risk': g('boardRisk'),
        'Compensation_Risk': g('compensationRisk'),
        'Overall_Risk': g('overallRisk'),
        # Predicted — yfinance
        'YF_Forward_PE': round_if(info.get('forwardPE'), 2),
        'YF_PEG': round_if(info.get('pegRatio'), 2),
        'YF_EPS_Forward': round_if(info.get('forwardEps'), 2),
        'YF_EPS_Current_Year': round_if(info.get('epsCurrentYear'), 2),
        'YF_Earnings_Date': epoch_to_date('earningsTimestamp'),
        'YF_Recommendation_Mean': round_if(info.get('recommendationMean'), 2),
        'YF_Num_Analysts': g('numberOfAnalystOpinions'),
        'YF_Target_High': round_if(info.get('targetHighPrice'), 2),
        'YF_Target_Low': round_if(info.get('targetLowPrice'), 2),
        'YF_Target_Mean': round_if(info.get('targetMeanPrice'), 2),
        'YF_Target_Median': round_if(info.get('targetMedianPrice'), 2),
        # Timestamp
        'YF_Updated_At': datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S'),
    }

    return data


def main():
    print(f"[{datetime.now(UTC8)}] Starting yfinance data collection...")

    sheet_ids = get_stock_sheet_ids()
    if not sheet_ids:
        print("ERROR: No stock sheet IDs found. Run create_stock_sheets.py first.")
        return
    print(f"Loaded {len(sheet_ids)} stock sheet mappings")

    service = get_sheets_service()
    sheets = service.spreadsheets().values()
    today = get_trading_date()

    first_sid = next(iter(sheet_ids.values()))
    header_map = get_header_map(sheets, first_sid)
    uni_header_map = get_universe_header_map(sheets)
    uni_ticker_rows = get_universe_ticker_rows(sheets)
    print(f"Header map: {len(header_map)} columns, Universe: {len(uni_header_map)} columns")

    last_write_time = 0.0
    universe_buffer = []
    updated = 0
    for i, (ticker, sid) in enumerate(sorted(sheet_ids.items()), 1):
        print(f"[{i}/{len(sheet_ids)}] {ticker}...", end=' ', flush=True)
        try:
            data = fetch_ticker_data(ticker)
            close = data.get('Close', '')
            print(f"${close}" if close else "no data", end=' ', flush=True)

            # Skip columns already written by other collectors (Finnhub first)
            row = find_or_create_today_row(sheets, sid, today)
            existing = read_existing_row(sheets, sid, row, header_map)
            filtered = {k: v for k, v in data.items()
                        if k not in existing or k == 'YF_Updated_At'}
            skipped = len(data) - len(filtered)
            if skipped:
                print(f"(skip {skipped} existing)", end=' ', flush=True)

            elapsed = time.monotonic() - last_write_time
            if elapsed < WRITE_INTERVAL:
                time.sleep(WRITE_INTERVAL - elapsed)

            write_stock_data(sheets, sid, row, header_map, filtered)
            last_write_time = time.monotonic()
            universe_buffer.append((ticker, filtered))

            if len(universe_buffer) >= UNIVERSE_BATCH_SIZE:
                elapsed = time.monotonic() - last_write_time
                if elapsed < WRITE_INTERVAL:
                    time.sleep(WRITE_INTERVAL - elapsed)
                batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
                last_write_time = time.monotonic()
                print(f"[universe batch {len(universe_buffer)} written]", flush=True)
                universe_buffer = []

            print(f"→ row {row}")
            updated += 1
        except Exception as e:
            print(f"ERROR: {e}")

    if universe_buffer:
        elapsed = time.monotonic() - last_write_time
        if elapsed < WRITE_INTERVAL:
            time.sleep(WRITE_INTERVAL - elapsed)
        batch_write_universe(sheets, uni_ticker_rows, uni_header_map, universe_buffer)
        print(f"[universe batch {len(universe_buffer)} written]")

    print(f"\n[{datetime.now(UTC8)}] Done! Updated {updated}/{len(sheet_ids)} sheets")


if __name__ == '__main__':
    main()
