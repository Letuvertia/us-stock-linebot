# US Stock LINE Bot

A LINE chat bot for US stock analysis — chat queries, scheduled portfolio reports, real-time news summarization. Hybrid system: Google Apps Script handles LINE bot interactions; Python collectors gather market data into Google Sheets.

## Features

**Chat queries** — `@bot 皮皮 ...`:
- 目標價 by industry, sub-industry, or single ticker (English / 中文 / code)
- P/E ranking by industry / sub-industry
- 新聞 by ticker (last 7 days, with live price injection)

**Scheduled reports**:
- Portfolio P&L (台股收盤 14:00, 美股收盤 04:30 Taiwan time) with live yfinance prices and aggregated NTD valuation
- Real-time news push: local Ollama summarizer → GAS webhook → LINE group as soon as new articles are processed

**Personality**: random 皮皮 flavor text — 10 help-card variants and 14 fallback reactions.

## Architecture

```
                       Google Sheets
                       (shared database)
                            ▲ ▼
                            │ │
   ┌────────────────────────┘ └────────────────────────┐
   │                                                   │
Python collectors                              GAS TypeScript bot
(GitHub Actions + local cron)                  (deployed via clasp)
                                                       │
                                                       ▼
                                                  LINE group
```

- **GAS** is *read-only* against Sheets; Python collectors own all writes.
- **Sheets** is the single source of truth — historical per-stock sheets + a `StockUniverse` snapshot tab + a metadata spreadsheet (`NewsSheetIDs`, `Config`, `StockSheetIDs`).

## Layout

```
us-stock-linebot/
├── src/                    GAS TypeScript bot
│   ├── main/               webhook entries (localWebhook, chatWebhook, trigger)
│   ├── report/             portfolio, newsSummary, target/PE rankings
│   ├── config/, util/      shared helpers
│   ├── package.json, tsconfig.json, .clasp.json, appsscript.json
│   └── (npm commands run from here)
├── scripts/                Python collectors
│   ├── config.py           centralized env vars + tuning constants (single source of truth)
│   ├── market_data/        Finnhub / yfinance / FMP / MarketWatch
│   ├── market_news/        CNBC / Reuters / summarizer
│   └── util/               one-off backfill / migration scripts
├── .github/workflows/      cicd.yaml + per-collector cron workflows
├── .secrets/               service-account JSONs (gitignored)
└── CLAUDE.md, README.md
```

## Data collectors

All written to per-stock spreadsheets *and* a `StockUniverse` snapshot tab in batched writes (20 tickers/call) to stay under the Sheets 60-write/min quota.

| Script | Schedule | What |
|---|---|---|
| `collect_finnhub.py` | hourly (skip 13:00–18:00 UTC+8) | quote, ratings, fundamentals (PE, EPS, ROE, ...) |
| `collect_yfinance.py` | daily 15:00 UTC+8 | OHLCV + dividends/splits |
| `collect_fmp.py` | daily 14:00 UTC+8 | analyst price targets |
| `collect_marketwatch.py` | local cron 13:00 / 14:30 / 16:00 / 17:30 UTC+8 | analyst estimates via `nodriver` + Xvfb (DataDome bypass; GitHub IPs are blocked, so this runs locally) |
| `collect_cnbc.py` | hourly | RSS → per-source news sheet |
| `collect_reuters.py` | hourly | RSS → per-source news sheet |
| `summarize_cnbc.py` | local cron `:05` hourly | local Ollama (qwen3.5:4b) → 繁體中文 summary + 利多/利空/中立 ticker tags → notify GAS webhook |

## CI/CD

`.github/workflows/cicd.yaml` — runs on every branch push:
1. `npm run build` — `tsc` compiles `src/**/*.ts` → `src/dist/`
2. `npm run push` — `clasp push` uploads to GAS
3. `npm run deploy` — `clasp deploy --deploymentId` updates the existing deployment, **same URL** (LINE webhook never needs reconfiguring)
4. **Install triggers** — `curl`s a token-guarded `doGet` endpoint to reconcile GAS triggers with `_triggerSpecs()` in `src/main/trigger.ts` (workaround for clasp's missing `script.scriptapp` OAuth scope)

**Versioning**:
- Push to `main` (after PR merge) auto-bumps the patch tag (e.g. `0.0.1` → `0.0.2`) and deploys with that tag as description
- Push to other branches deploys as `<latest-tag>-<short-sha>` (preview build on the same script)
- Manual `release.yaml` (workflow_dispatch with version input) tags + creates a GitHub release + deploys with that exact version

## Setup

### Required env vars

The Python config layer (`scripts/config.py`) is strict — missing required vars raise a clear `RuntimeError` on import, not a vague auth error mid-run.

| Var | Purpose |
|---|---|
| `DATA_CREDS_FILE` | service account JSON for metadata + per-stock spreadsheets |
| `NEWS_CREDS_FILE` | service account JSON for news spreadsheets |
| `US_STOCK_SPREADSHEET_ID` | metadata spreadsheet (StockUniverse, NewsSheetIDs, Config) |
| `USER_CONFIG_SPREADSHEET_ID` | user-config spreadsheet (industry tags, watchlist) |
| `FMP_API_KEY` | comma-separated keys for round-robin (FMP collector only) |
| `FINNHUB_API_KEY` | comma-separated keys for round-robin (Finnhub collector only) |

Tuning constants (Ollama model, MW delays, lookback days) live in `scripts/config.py` — edit there to change them, no env override.

### Local development

```bash
# Python: place creds at .secrets/, populate .env, run scripts manually
cp /path/to/data-creds.json .secrets/data-creds.json
cp /path/to/news-creds.json .secrets/news-creds.json
cat > .env <<EOF
DATA_CREDS_FILE=$(pwd)/.secrets/data-creds.json
NEWS_CREDS_FILE=$(pwd)/.secrets/news-creds.json
US_STOCK_SPREADSHEET_ID=...
USER_CONFIG_SPREADSHEET_ID=...
EOF

# GAS TypeScript: build + push from src/
cd src
npm ci
npm run build           # compile only
npm run push            # build + clasp push (uploads to GAS)
```

### CI secrets (GitHub repo → Settings → Secrets)

`GOOGLE_SERVICE_ACCOUNT_KEY`, `NEWS_SERVICE_ACCOUNT_KEY`, `US_STOCK_SPREADSHEET_ID`, `USER_CONFIG_SPREADSHEET_ID`, `FMP_API_KEY`, `FINNHUB_API_KEY`, `CLASP_TOKEN`, `GAS_DEPLOYMENT_ID`, `INSTALL_TRIGGERS_TOKEN`.

### GAS Script Properties

`US_STOCK_METADATA_SPREADSHEET_ID`, `LINEBOT_LOGS_SPREADSHEET_ID`, `USER_CONFIG_SPREADSHEET_ID`, `LINE_CHANNEL_ACCESS_TOKEN`, `LINE_CHANNEL_SECRET`, `LINE_GROUP_ID`, `INSTALL_TRIGGERS_TOKEN`.

## Developer workflow

All changes go through a branch + PR. Each PR ends with **exactly one commit** (squash via `git cpf`). Direct push to `main` is blocked by branch protection.

```bash
git checkout -b feat/<topic>      # or fix/<topic>, chore/<topic>
# ...edit, build, test...
git commit -m "feat: ..."
git push -u origin feat/<topic>
gh pr create --base main --title "[main] feat: ..." --body "..."
# iterate: edit + git cpf (alias for amend + force-push)
# merge via GitHub UI; cicd auto-bumps tag and deploys
```

See `CLAUDE.md` for the full developer guide.

## Inspiration / acknowledgements

皮皮 (Pípí) is a real dog. The ack and reaction messages are observations of her actual behavior.
