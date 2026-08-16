# US Stock LINE Bot

A comprehensive LINE chat bot and market intelligence system for US equities — featuring interactive chat queries, scheduled portfolio performance reports, real-time news summarization, and automated financial podcast digests.

The system is built as a hybrid architecture: **Google Apps Script (GAS TypeScript)** powers the interactive LINE bot webhook and scheduled broadcast reports, while **Python collectors (GitHub Actions + local cron)** collect market data and summarize media into **Google Sheets** as the shared database.

---

## Features

### 1. Interactive Chat Queries (`@bot 皮皮 ...`)
Mention the bot in your LINE group using natural chat commands:
* **Analyst Price Targets (`目標價`)**:
  * Query by ticker: `@bot 皮皮 目標價 NVDA`
  * Query by industry/sub-industry (English, Traditional Chinese, or industry code): `@bot 皮皮 目標價 半導體` or `@bot 皮皮 目標價 Semiconductors`
  * Displays current price, mean target, high/low targets, consensus rating, and upside/downside percentage.
* **P/E Multiples & Valuation (`本益比`)**:
  * Query industry/sub-industry valuation rankings: `@bot 皮皮 本益比 軟體`
  * Shows trailing and forward PE ratios across industry peers.
* **Recent News & Sentiment (`新聞`)**:
  * Query news for a specific stock: `@bot 皮皮 新聞 AAPL`
  * Specify custom lookback window (e.g. last 3 days): `@bot 皮皮 新聞 TSLA 3`
  * Features real-time price injection and sentiment tagging (利多 / 利空 / 中立).
* **Portfolio Holdings & Performance (`持股` / `持倉`)**:
  * Query current portfolio status: `@bot 皮皮 持股` or `@bot 皮皮 倉位`
  * Displays total valuation (NTD), daily P&L, overall ROI, cash/loan balances, and individual stock gains/losses.
* **Help & Bot Personality**:
  * `@bot 皮皮 說明` or `@bot 皮皮 help`
  * Includes randomized 皮皮 (Pípí) dog personality flavor text (10+ greeting variants and fallback responses).

### 2. Scheduled Broadcast Reports
* **Portfolio P&L Reports**:
  * **Taiwan Market Close (台股收盤)**: Scheduled daily at 14:00 UTC+8.
  * **US Market Close (美股收盤)**: Scheduled daily at 04:30 UTC+8.
  * Reads holdings from `UserHoldings` tab, fetches live prices via yfinance, and provides aggregated NTD portfolio valuation, daily change, and individual position P&L.
* **Real-time News Summarization**:
  * Automatically summarizes breaking CNBC/Reuters articles via local LLM (`qwen3.5:4b`).
  * Triggers an immediate push to the LINE group as soon as new articles are processed.
* **Podcast Intelligence Reports**:
  * **Gooaye (股癌)** & **RichWomen (代代財女)**:
  * Automatically ingests RSS episodes, transcribes audio via `faster-whisper`, extracts structured key takeaways with LLM, and pushes concise episode digests to LINE.

---

## Architecture & Data Flow

```
                      ┌────────────────────────────────────────┐
                      │             Google Sheets              │
                      │     (Shared Central Database)          │
                      │  - Metadata (StockUniverse, IDs)       │
                      │  - 500 Historical Stock Sheets         │
                      │  - News & Podcast Stores               │
                      └──────────────┬──────────────────▲──────┘
                                     │                  │
                      Read-only Data │                  │ Batch Writes
                                     ▼                  │
┌──────────────────────────────────────┐     ┌──────────┴───────────────────────────┐
│          GAS TypeScript Bot          │     │          Python Collectors           │
│             (src/)                   │     │              (scripts/)              │
│                                      │     │                                      │
│ - LINE Webhook (doPost)              │     │ - Market Data: Finnhub, YF, FMP, MW  │
│ - Scheduled Reports (doGet Triggers) │     │ - Market News: CNBC, Reuters + LLM   │
│ - Target / PE / Portfolio Rankings   │     │ - Podcasts: Gooaye & RichWomen       │
└──────────────────┬───────────────────┘     └──────────────────────────────────────┘
                   │
                   │ Push & Reply Messages
                   ▼
         ┌───────────────────┐
         │    LINE Group     │
         └───────────────────┘
```

### Core Architecture Principles
1. **Google Sheets as Database**: Sheets acts as the single source of truth across all components.
2. **Strict Separation of Concerns**:
   * **Python collectors** own all write operations (historical quotes, universe updates, news records). Batch writes (20 tickers per batch) are enforced to stay well within Google Sheets' 60 writes/minute quota.
   * **GAS TypeScript** is strictly **read-only** against Google Sheets, guaranteeing that chat queries and scheduled broadcasts never contend for write quotas.
3. **Resilience & Retry**: All external API integrations in both GAS (`retryWithBackoff`) and Python (`*_with_retry`) use exponential backoff and error logging.

---

## Repository Layout

```
us-stock-linebot/
├── src/                    # GAS TypeScript bot (LINE webhook handlers, scheduled reports)
│   ├── main/               # Webhook entries (localWebhook, chatWebhook, trigger)
│   ├── report/             # Portfolio, newsSummary, target/PE rankings
│   ├── config/             # Constants & configuration keys
│   ├── util/               # Shared GAS helpers (line, sheets, logger, retry)
│   ├── package.json        # Dependencies & npm scripts
│   ├── tsconfig.json       # TypeScript configuration (module: "None")
│   ├── .clasp.json         # clasp configuration linking to Apps Script
│   └── appsscript.json     # Apps Script manifest and OAuth scopes
├── scripts/                # Python collectors & pipelines
│   ├── config.py           # Centralized environment variables & constants
│   ├── market_data/        # Finnhub, yfinance, FMP, MarketWatch collectors
│   ├── market_news/        # CNBC, Reuters RSS collectors, Ollama summarizer, cleanup
│   ├── podcasts/           # Gooaye (股癌) and RichWomen (代代財女) pipelines
│   │   ├── gooaye/         # RSS collect, download, transcribe, summarize
│   │   ├── richwomen/      # RSS collect, download, transcribe, summarize
│   │   └── podcast_common.py # Shared podcast utilities
│   ├── portfolio/          # Portfolio calculation helpers
│   └── util/               # One-off backfill and migration utilities
├── podcasts_data/          # Downloaded MP3s and transcript txt files (gitignored)
├── .github/workflows/      # CI/CD (cicd.yaml, release.yaml) & collector cron workflows
├── .secrets/               # Service account JSON keys (gitignored)
├── AGENTS.md               # Developer guide & operational runbook
└── README.md               # Product & domain specifications
```

---

## Data Pipelines & Collectors

### 1. Market Data Collectors (`scripts/market_data/`)
| Script | Schedule (UTC+8) | Source | Output / Purpose |
|---|---|---|---|
| `collect_finnhub.py` | Hourly (skip 14:00–16:00) | Finnhub API | Real-time quote, analyst recommendation trends, basic financial metrics (PE, EPS, ROE, 52w high/low). |
| `collect_yfinance.py` | Daily 15:00 | Yahoo Finance | Daily OHLCV bars, splits, and dividend records. |
| `collect_fmp.py` | Daily 14:00 | Financial Modeling Prep | Consensus analyst price targets (mean, high, low, consensus rating). |
| `collect_marketwatch.py` | Local Cron 13:00, 14:30, 16:00, 17:30 | MarketWatch Web | Analyst forward estimates via `nodriver` (undetected Chrome). Resumes from `.mw_progress.json`. Requires residential IP. |

### 2. Market News Collectors (`scripts/market_news/`)
| Script | Schedule (UTC+8) | Source | Output / Purpose |
|---|---|---|---|
| `collect_cnbc.py` | Hourly :00 | CNBC RSS | Ingests latest market news; extracts ticker tags via keyword matching. |
| `collect_reuters.py` | Hourly :30 | Reuters RSS | Ingests global business and technology news feeds. |
| `summarize_cnbc.py` | Local Cron Hourly :05 | Local Ollama (`qwen3.5:4b`) | Generates Traditional Chinese summaries, sentiment ratings, and notifies GAS webhook for instant push. |
| `cleanup_news.py` | Daily 02:47 | Google Sheets | Housekeeping job deleting news records older than 7 days. |

### 3. Podcast Pipelines (`scripts/podcasts/`)
| Podcast Show | Ingestion Cron | Download Cron | Transcribe Cron | Summarize & Push |
|---|---|---|---|---|
| **Gooaye (股癌)** | 17:00 UTC+8 | 17:30 UTC+8 | 18:00 UTC+8 (`faster-whisper` medium) | 19:00 UTC+8 (Ollama → GAS webhook) |
| **RichWomen (代代財女)** | 17:05 UTC+8 | 17:35 UTC+8 | 18:05 UTC+8 (`faster-whisper` medium) | 19:05 UTC+8 (Ollama → GAS webhook) |

---

## Google Sheets Database Structure

All spreadsheets are stored under Google Drive directory `1kpHXJlv4Abb_S6J8vTSUv44FOQEzDPMu`:

1. **Main Metadata Spreadsheet (`US_STOCK_SPREADSHEET_ID`)**:
   * `StockUniverse`: Master list of tracked tickers, company names, latest market snapshot metrics, target prices, and collector update timestamps (`Finnhub_Updated_At`, `FMP_Updated_At`, `MW_Updated_At`, `YF_Updated_At`).
   * `StockSheetIDs`: Maps each ticker symbol to its individual spreadsheet ID.
   * `NewsSheetIDs`: Maps news source names (`CNBC`, `Reuters`) to their respective spreadsheet IDs.
   * `PodcastSheetIDs`: Maps podcast channel names (`Gooaye`, `RichWomen`) to their transcript/summary spreadsheet IDs.
   * `Data Schema`: Master definition and data dictionary for historical daily stock metrics.
2. **Individual Stock Spreadsheets (`/stocks/{TICKER} - {Company Name}`)**:
   * Over 500 individual spreadsheets, each containing a `Daily` tab recording historical end-of-day quotes and fundamentals (one row per US trading day).
3. **News Spreadsheets (`CNBC`, `Reuters`)**:
   * Columns: `ID`, `Date`, `TickerTags`, `Title`, `Content`, `URL`, `Processed_At`, `Summary`.
4. **Podcast Spreadsheets (`Gooaye`, `RichWomen`)**:
   * Columns: `ID`, `Date`, `Title`, `Duration`, `AudioURL`, `EpisodeURL`, `DownloadedAt`, `LocalMP3`, `TranscribedAt`, `LocalTXT`, `Summary`.
5. **User Config Spreadsheet (`USER_CONFIG_SPREADSHEET_ID`)**:
   * `UserHoldings`: User portfolio positions, cost bases, and loan configurations.
   * `UserHoldingTransactions`: Historical trade, dividend, and foreign exchange transactions.
   * `News Keywords`: Custom keyword and alias dictionary for news ticker tagging to avoid false positives on short tickers (e.g. `A`, `ON`, `IT`).
   * `Industry Category`: Standardized and customized sector/industry categorizations.
6. **System Logs Spreadsheet (`LINEBOT_LOGS_SPREADSHEET_ID`)**:
   * `Logs`: Execution traces, user chat events, and errors emitted by GAS.

---

## Configuration & Environment Variables

### Python Collectors Configuration (`scripts/config.py`)
Centralized configuration with fail-fast validation on startup:
* `DATA_CREDS_FILE`: Path to Google Service Account JSON for metadata and stock sheets.
* `NEWS_CREDS_FILE`: Path to Google Service Account JSON for news spreadsheets.
* `US_STOCK_SPREADSHEET_ID`: Main metadata spreadsheet ID.
* `USER_CONFIG_SPREADSHEET_ID`: User configuration spreadsheet ID.
* `FMP_API_KEY`: Comma-separated Financial Modeling Prep API keys (round-robin).
* `FINNHUB_API_KEY`: Comma-separated Finnhub API keys (round-robin).

### Google Apps Script Properties
Configured in Google Apps Script Project Settings:
* `US_STOCK_METADATA_SPREADSHEET_ID`
* `LINEBOT_LOGS_SPREADSHEET_ID`
* `USER_CONFIG_SPREADSHEET_ID`
* `LINE_CHANNEL_ACCESS_TOKEN`
* `LINE_CHANNEL_SECRET`
* `LINE_GROUP_ID`
* `INSTALL_TRIGGERS_TOKEN`

---

## Development & Contribution Guide

For instructions on setting up local development, running collectors, debugging via GCP/clasp logs, Git branch & PR standards, and CI/CD deployment mechanics, please refer to:

👉 **[AGENTS.md](file:///home/letuvertia/us-stock-linebot/AGENTS.md)** (Developer Guide & Operational Runbook)

---

## Inspiration & Acknowledgements

皮皮 (Pípí) is a real dog. Her playful personality, acknowledgment messages, and reaction cards are direct reflections of her daily antics.

