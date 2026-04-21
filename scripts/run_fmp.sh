#!/bin/bash
# Daily FMP price target collector
cd /mnt/c/Users/1026o/Desktop/us-stock-linebot
python3 scripts/collect_fmp_targets.py >> scripts/fmp.log 2>&1
