#!/bin/bash
# MarketWatch analyst estimates collector (batch mode)
# Usage: MW_BATCH=first ./scripts/run_mw.sh
#        MW_BATCH=second ./scripts/run_mw.sh
cd /mnt/c/Users/1026o/Desktop/us-stock-linebot
MW_BATCH=${MW_BATCH:-all} python3 scripts/collect_marketwatch.py >> scripts/marketwatch.log 2>&1
