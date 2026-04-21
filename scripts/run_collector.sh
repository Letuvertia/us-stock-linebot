#!/bin/bash
# Hourly Finnhub data collector for StockUniverse
cd /mnt/c/Users/1026o/Desktop/us-stock-linebot
python3 scripts/collect_finnhub.py >> scripts/collector.log 2>&1
