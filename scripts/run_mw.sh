#!/bin/bash
# Daily MarketWatch analyst estimates collector
cd /mnt/c/Users/1026o/Desktop/us-stock-linebot
python3 scripts/collect_marketwatch.py >> scripts/marketwatch.log 2>&1
