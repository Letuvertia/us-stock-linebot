#!/usr/bin/env python3
"""Hourly Finnhub collector. Writes to individual stock sheets in /stocks folder."""
import os
import sys
import time
import json
import urllib.request
import urllib.error
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (
    UTC8, get_sheets_service, get_stock_sheet_ids, get_header_map,
    find_or_create_today_row, write_stock_data, round_if, _is_retryable,
    get_trading_date, get_universe_ticker_rows, get_universe_header_map,
    write_universe_row,
)

FINNHUB_KEYS = [k.strip() for k in os.environ['FINNHUB_API_KEY'].split(',') if k.strip()]
FINNHUB_BASE = 'https://finnhub.io/api/v1'
RATE_LIMIT_DELAY = 0.4
_key_index = 0
_dead_keys = set()


def finnhub_get(endpoint: str, max_retries: int = 3) -> dict | list | None:
    global _key_index
    if len(_dead_keys) >= len(FINNHUB_KEYS):
        print("  All API keys exhausted!")
        return None
    for attempt in range(max_retries * len(FINNHUB_KEYS)):
        while FINNHUB_KEYS[_key_index % len(FINNHUB_KEYS)] in _dead_keys:
            _key_index += 1
            if len(_dead_keys) >= len(FINNHUB_KEYS):
                return None
        token = FINNHUB_KEYS[_key_index % len(FINNHUB_KEYS)]
        url = f"{FINNHUB_BASE}{endpoint}{'&' if '?' in endpoint else '?'}token={token}"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    _key_index += 1
                    return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                key_num = _key_index % len(FINNHUB_KEYS) + 1
                _dead_keys.add(token)
                print(f"  Key {key_num} rate limited, switching...", end=' ', flush=True)
                _key_index += 1
                if len(_dead_keys) >= len(FINNHUB_KEYS):
                    print("all keys exhausted!")
                    return None
                continue
            print(f"  HTTP {e.code} for {endpoint}")
            return None
        except Exception as e:
            print(f"  Error: {e}")
            return None
    return None


def calc_rating_score(rec: dict) -> float:
    sb, b, h, s, ss = (rec.get(k, 0) for k in ('strongBuy', 'buy', 'hold', 'sell', 'strongSell'))
    total = sb + b + h + s + ss
    if total == 0:
        return ''
    return round((sb * 5 + b * 4 + h * 3 + s * 2 + ss * 1) / total, 2)


def fetch_ticker_data(ticker: str) -> dict:
    """Fetch all Finnhub data for a single ticker, return column_name→value dict."""
    now = datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')

    # 1. Quote
    q = finnhub_get(f'/quote?symbol={ticker}') or {}
    time.sleep(RATE_LIMIT_DELAY)

    current_price = q.get('c', '')

    # 2. Metrics
    metric_data = finnhub_get(f'/stock/metric?symbol={ticker}&metric=all') or {}
    m = metric_data.get('metric', {})
    time.sleep(RATE_LIMIT_DELAY)

    # 3. Recommendations
    recs = finnhub_get(f'/stock/recommendation?symbol={ticker}') or []
    rec = recs[0] if recs else {}
    time.sleep(RATE_LIMIT_DELAY)

    # 4. Earnings (last quarter)
    earnings = finnhub_get(f'/stock/earnings?symbol={ticker}') or []
    e = earnings[0] if earnings else {}
    time.sleep(RATE_LIMIT_DELAY)

    # 5. Company profile
    profile = finnhub_get(f'/stock/profile2?symbol={ticker}') or {}
    time.sleep(RATE_LIMIT_DELAY)

    # 6. Peers
    peers_list = finnhub_get(f'/stock/peers?symbol={ticker}') or []
    time.sleep(RATE_LIMIT_DELAY)

    # 7. Insider transactions
    insider_data = finnhub_get(f'/stock/insider-transactions?symbol={ticker}') or {}
    insider_txns = insider_data.get('data', [])
    latest_insider = insider_txns[0] if insider_txns else {}
    time.sleep(RATE_LIMIT_DELAY)

    # 8. Earnings calendar (next earnings date)
    from datetime import timedelta
    today_str = datetime.now(UTC8).strftime('%Y-%m-%d')
    future_str = (datetime.now(UTC8) + timedelta(days=90)).strftime('%Y-%m-%d')
    cal = finnhub_get(f'/calendar/earnings?symbol={ticker}&from={today_str}&to={future_str}') or {}
    cal_earnings = cal.get('earningsCalendar', [])
    next_earnings_date = cal_earnings[0].get('date', '') if cal_earnings else ''
    time.sleep(RATE_LIMIT_DELAY)

    # Dist from high
    w52_high = m.get('52WeekHigh', '')
    dist = ''
    if current_price and w52_high and w52_high > 0:
        dist = round(((current_price - w52_high) / w52_high) * 100, 2)

    data = {
        # Quote
        'Current_Price': current_price,
        'Change_Pct': q.get('dp', ''),
        'Prev_Close': q.get('pc', ''),
        # 52-week range
        '52W_High': w52_high,
        '52W_High_Date': m.get('52WeekHighDate', ''),
        '52W_Low': m.get('52WeekLow', ''),
        '52W_Low_Date': m.get('52WeekLowDate', ''),
        'Dist_From_High_Pct': dist,
        # Returns
        '5D_Price_Return': m.get('5DayPriceReturnDaily', ''),
        'MTD_Price_Return': m.get('monthToDatePriceReturnDaily', ''),
        '13W_Price_Return': m.get('13WeekPriceReturnDaily', ''),
        '26W_Price_Return': m.get('26WeekPriceReturnDaily', ''),
        '52W_Price_Return': m.get('52WeekPriceReturnDaily', ''),
        'YTD_Price_Return': m.get('yearToDatePriceReturnDaily', ''),
        'Price_Rel_SP500_4W': m.get('priceRelativeToS&P5004Week', ''),
        'Price_Rel_SP500_13W': m.get('priceRelativeToS&P50013Week', ''),
        'Price_Rel_SP500_26W': m.get('priceRelativeToS&P50026Week', ''),
        'Price_Rel_SP500_52W': m.get('priceRelativeToS&P50052Week', ''),
        'Price_Rel_SP500_YTD': m.get('priceRelativeToS&P500Ytd', ''),
        # Volatility
        'Beta': round_if(m.get('beta'), 4),
        '3M_Avg_Daily_Return_Std': round_if(m.get('3MonthADReturnStd'), 2),
        # Volume averages
        '10D_Avg_Volume': round_if(m.get('10DayAverageTradingVolume'), 2),
        '3M_Avg_Volume': round_if(m.get('3MonthAverageTradingVolume'), 2),
        # Valuation
        'PE_TTM': round_if(m.get('peTTM'), 2),
        'PB': round_if(m.get('pbQuarterly'), 2),
        'PS_TTM': round_if(m.get('psTTM'), 2),
        'P_CF_TTM': round_if(m.get('pcfShareTTM'), 2),
        'P_FCF_TTM': round_if(m.get('pfcfShareTTM'), 2),
        'EV_EBITDA_TTM': round_if(m.get('evEbitdaTTM'), 2),
        'EV_Revenue_TTM': round_if(m.get('evRevenueTTM'), 2),
        'Enterprise_Value': round_if(m.get('enterpriseValue'), 2),
        'Price_To_Tangible_BV': round_if(m.get('ptbvQuarterly'), 2),
        # EPS
        'EPS_TTM': m.get('epsTTM', ''),
        'EPS_Annual': m.get('epsAnnual', ''),
        'EPS_Growth_QoQ': m.get('epsGrowthQuarterlyYoy', ''),
        'EPS_Growth_TTM_YoY': m.get('epsGrowthTTMYoy', ''),
        'EPS_Growth_3Y': m.get('epsGrowth3Y', ''),
        'EPS_Growth_5Y': m.get('epsGrowth5Y', ''),
        # Quarterly earnings
        'EPS_Q_Actual': e.get('actual', ''),
        'EPS_Q_Estimate': e.get('estimate', ''),
        'EPS_Q_Surprise': e.get('surprise', ''),
        'EPS_Q_Surprise_Pct': e.get('surprisePercent', ''),
        # Per-share
        'EBITDA_Per_Share': round_if(m.get('ebitdPerShareTTM'), 2),
        'Revenue_Per_Share': round_if(m.get('revenuePerShareTTM'), 2),
        # Revenue growth
        'Revenue_Growth_QoQ': m.get('revenueGrowthQuarterlyYoy', ''),
        'Revenue_Growth_TTM_YoY': m.get('revenueGrowthTTMYoy', ''),
        'Revenue_Growth_3Y': m.get('revenueGrowth3Y', ''),
        'Revenue_Growth_5Y': m.get('revenueGrowth5Y', ''),
        'Revenue_Per_Employee': round_if(m.get('revenueEmployeeTTM'), 2),
        # Margins
        'Gross_Margin': m.get('grossMarginTTM', ''),
        'Gross_Margin_5Y': m.get('grossMargin5Y', ''),
        'Operating_Margin': m.get('operatingMarginTTM', ''),
        'Operating_Margin_5Y': m.get('operatingMargin5Y', ''),
        'Net_Margin': m.get('netProfitMarginTTM', ''),
        'Net_Margin_5Y': m.get('netProfitMargin5Y', ''),
        'Pretax_Margin': m.get('pretaxMarginTTM', ''),
        # Returns on capital
        'ROE': m.get('roeTTM', ''),
        'ROE_5Y': m.get('roe5Y', ''),
        'ROA': m.get('roaTTM', ''),
        'ROA_5Y': m.get('roa5Y', ''),
        'ROI': m.get('roiTTM', ''),
        'ROI_5Y': m.get('roi5Y', ''),
        # Financial health
        'Current_Ratio': m.get('currentRatioQuarterly', ''),
        'Quick_Ratio': m.get('quickRatioQuarterly', ''),
        'Debt_To_Equity': round_if(m.get('totalDebt/totalEquityQuarterly'), 2),
        'LT_Debt_To_Equity': round_if(m.get('longTermDebt/equityQuarterly'), 2),
        'Interest_Coverage': round_if(m.get('netInterestCoverageTTM'), 2),
        'Total_Cash_Per_Share': round_if(m.get('cashPerSharePerShareQuarterly'), 2),
        # Efficiency
        'Asset_Turnover': round_if(m.get('assetTurnoverTTM'), 2),
        'Inventory_Turnover': round_if(m.get('inventoryTurnoverTTM'), 2),
        'Receivables_Turnover': round_if(m.get('receivablesTurnoverTTM'), 2),
        # Cash flow
        'Cash_Flow_Per_Share': round_if(m.get('cashFlowPerShareTTM'), 2),
        'FCF_CAGR_5Y': m.get('focfCagr5Y', ''),
        'CapEx_CAGR_5Y': m.get('capexCagr5Y', ''),
        'EBITDA_CAGR_5Y': m.get('ebitdaCagr5Y', ''),
        # Dividends
        'Dividend_Yield': round_if(m.get('dividendYieldIndicatedAnnual'), 4),
        'Dividend_Per_Share_TTM': round_if(m.get('dividendPerShareTTM'), 4),
        'Payout_Ratio': round_if(m.get('payoutRatioTTM'), 2),
        'Dividend_Growth_5Y': m.get('dividendGrowthRate5Y', ''),
        # Size
        'Market_Cap_M': round_if(m.get('marketCapitalization'), 2),
        # Per-share book value
        'Book_Value_Per_Share': round_if(m.get('bookValuePerShareQuarterly'), 2),
        'Tangible_BV_Per_Share': round_if(m.get('tangibleBookValuePerShareQuarterly'), 2),
        'BV_Share_Growth_5Y': m.get('bookValueShareGrowth5Y', ''),
        'Net_Income_Per_Employee': round_if(m.get('netIncomeEmployeeTTM'), 4),
        # Predicted — Finnhub
        'Finnhub_Forward_PE': round_if(m.get('forwardPE'), 2),
        'Finnhub_PEG': round_if(m.get('pegTTM'), 2),
        'Finnhub_StrongBuy': rec.get('strongBuy', ''),
        'Finnhub_Buy': rec.get('buy', ''),
        'Finnhub_Hold': rec.get('hold', ''),
        'Finnhub_Sell': rec.get('sell', ''),
        'Finnhub_StrongSell': rec.get('strongSell', ''),
        'Finnhub_Rating_Score': calc_rating_score(rec) if rec else '',
        # Profile
        'Industry': profile.get('finnhubIndustry', ''),
        'IPO_Date': profile.get('ipo', ''),
        # Peers
        'Peers': ','.join(peers_list) if peers_list else '',
        # Insider transactions (most recent)
        'Insider_Tx_Latest_Date': latest_insider.get('transactionDate', ''),
        'Insider_Tx_Latest_Name': latest_insider.get('name', ''),
        'Insider_Tx_Latest_Change': latest_insider.get('change', ''),
        # Next earnings
        'Finnhub_Next_Earnings_Date': next_earnings_date,
        # Timestamp
        'Finnhub_Updated_At': now,
    }

    return data


def main():
    print(f"[{datetime.now(UTC8)}] Starting Finnhub data collection...")

    sheet_ids = get_stock_sheet_ids()
    if not sheet_ids:
        print("ERROR: No stock sheet IDs found. Run create_stock_sheets.py first.")
        return
    print(f"Loaded {len(sheet_ids)} stock sheet mappings")

    service = get_sheets_service()
    sheets = service.spreadsheets().values()
    today = get_trading_date()

    # Read header maps
    first_sid = next(iter(sheet_ids.values()))
    header_map = get_header_map(sheets, first_sid)
    uni_header_map = get_universe_header_map(sheets)
    uni_ticker_rows = get_universe_ticker_rows(sheets)
    print(f"Header map: {len(header_map)} columns, Universe: {len(uni_header_map)} columns")

    updated = 0
    for i, (ticker, sid) in enumerate(sorted(sheet_ids.items()), 1):
        print(f"[{i}/{len(sheet_ids)}] {ticker}...", end=' ', flush=True)
        try:
            data = fetch_ticker_data(ticker)
            price = data.get('Current_Price', '')
            print(f"${price}" if price else "no quote", end=' ', flush=True)

            row = find_or_create_today_row(sheets, sid, today)
            write_stock_data(sheets, sid, row, header_map, data)
            write_universe_row(sheets, uni_ticker_rows, uni_header_map, ticker, data)
            print(f"→ row {row}")
            updated += 1
        except Exception as e:
            print(f"ERROR: {e}")

    print(f"\n[{datetime.now(UTC8)}] Done! Updated {updated}/{len(sheet_ids)} sheets")


if __name__ == '__main__':
    main()
