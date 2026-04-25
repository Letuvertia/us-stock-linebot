# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Deploy

```bash
npm run build          # Compile TypeScript to dist/, copy appsscript.json
npm run push           # Build + deploy to GAS via clasp
```

Pushing to `main` auto-deploys GAS via `.github/workflows/deploy.yml`.

Python scripts have no build step. They run directly via GitHub Actions or local cron.

## Architecture

Hybrid system: **GAS (TypeScript)** handles LINE bot interactions, **Python scripts (GitHub Actions)** collect data. Google Sheets is the shared database.

### Data Flow

```
Python collectors (GitHub Actions / cron)
  → write to Google Sheets (StockUniverse, NewsStore)
    → GAS reads, ranks, formats
      → pushes to LINE group
```

### GAS TypeScript (`src/`)

Compiles with `module: "None"` — all functions share global scope (GAS requirement). No imports/exports.

- **`main.ts`** — Entry points: `doPost()` (webhook), `runPreMarketScan()`, `runPostMarketScan()`, `runNewsAnalysis()` (disabled), `installTriggers()`
- **`modules/moduleA.ts`** — Stock scanner: loads StockUniverse, ranks by upside, pushes top 20 to LINE
- **`modules/moduleB.ts`** — News analysis via OpenAI (disabled due to free tier limits)
- **`modules/moduleC.ts`** — Webhook chat handler: responds to @mentions using OpenAI
- **`services/geminiService.ts`** — Despite filename, uses **OpenAI GPT-4o-mini**. Contains `callOpenAI()`, prompt builders, chat handler
- **`services/stockService.ts`** — Stock ranking logic, `formatStockRanking()` with 皮皮 flavor text (random road + location)
- **`services/lineService.ts`** — LINE webhook verification, message send/reply with chunking
- **`services/sheetService.ts`** — Google Sheets CRUD helpers
- **`services/newsService.ts`** — `getNewsForTicker()` reads from NewsStore
- **`config.ts`** — API URLs, sheet names/headers, script property keys, constants

### Python Scripts (`scripts/`)

All use Google service account auth. Timestamps use UTC+8.

| Script | Schedule | What it does |
|---|---|---|
| `collect_finnhub.py` | Hourly | Quote, ratings, metrics → cols D-AE |
| `collect_fmp_targets.py` | Daily 8:03 AM | Price targets → cols AF-AK (batched by day parity) |
| `collect_marketwatch.py` | Local cron 7:03/13:03 | Scrapes analyst estimates → cols AL-AW (batched 250 each) |
| `collect_news.py` | Hourly | RSS feeds → NewsStore (keyword-based ticker tagging) |
| `cleanup_news.py` | Daily 2:47 AM | Deletes NewsStore rows older than 7 days |
| `backfill_keywords.py` | One-time | Populates Keywords col (AX) from company names |
| `backfill_exchange_name.py` | One-time | Populates Exchange/Name from Finnhub profile2 |

### Google Sheets Schema

**StockUniverse** (50 columns): Ticker, Exchange, Name, Finnhub data (D-AE), FMP targets (AF-AK), MarketWatch data (AL-AW), Keywords (AX)

**NewsStore**: ID, Date, TickerTags, Title, Snippet, URL, Processed_At

**UserConfig**: Config_Key, Config_Value (stores watchlist as JSON)

**SystemLogs**: Timestamp, Level, Function, Message

## Key Patterns

- **Retry with backoff**: `retryWithBackoff()` in GAS, `*_with_retry()` in Python — all external API calls use retry
- **Batch processing**: FMP and MarketWatch split 500 tickers into batches of 250 to respect rate limits
- **News tagging**: `extract_ticker_tags()` matches article text against Keywords column (AX) only, not ticker symbols, to avoid false positives from short tickers (A, IT, ON)
- **MarketWatch anti-bot**: Chrome 136 User-Agent, Sec-Fetch headers, 8s delay. GitHub Actions IPs are blocked — runs via local cron only
- **Sheets quota**: 60 writes/min. Python scripts use batch writes or rate-limit pauses

## Secrets & Config

**GAS Script Properties** (set in Apps Script editor): SPREADSHEET_ID, FINNHUB_API_KEY, LINE_CHANNEL_ACCESS_TOKEN, LINE_CHANNEL_SECRET, LINE_GROUP_ID, OPENAI_API_KEY

**GitHub Actions Secrets**: GOOGLE_SERVICE_ACCOUNT_KEY, SPREADSHEET_ID, FMP_API_KEY, FINNHUB_API_KEY, CLASP_TOKEN

## Cron Schedules (all times UTC+8)

- Finnhub: hourly at :07
- News collection: hourly at :13
- FMP targets: 8:03 AM daily
- MarketWatch: 7:03 AM + 1:03 PM (local cron only)
- News cleanup: 2:47 AM daily
- GAS pre-market scan: 20:30
- GAS post-market scan: 05:30

<!-- gitnexus:start -->
# GitNexus — Code Intelligence

This project is indexed by GitNexus as **us-stock-linebot** (542 symbols, 841 relationships, 32 execution flows). Use the GitNexus MCP tools to understand code, assess impact, and navigate safely.

> If any GitNexus tool warns the index is stale, run `npx gitnexus analyze` in terminal first.

## Always Do

- **MUST run impact analysis before editing any symbol.** Before modifying a function, class, or method, run `gitnexus_impact({target: "symbolName", direction: "upstream"})` and report the blast radius (direct callers, affected processes, risk level) to the user.
- **MUST run `gitnexus_detect_changes()` before committing** to verify your changes only affect expected symbols and execution flows.
- **MUST warn the user** if impact analysis returns HIGH or CRITICAL risk before proceeding with edits.
- When exploring unfamiliar code, use `gitnexus_query({query: "concept"})` to find execution flows instead of grepping. It returns process-grouped results ranked by relevance.
- When you need full context on a specific symbol — callers, callees, which execution flows it participates in — use `gitnexus_context({name: "symbolName"})`.

## Never Do

- NEVER edit a function, class, or method without first running `gitnexus_impact` on it.
- NEVER ignore HIGH or CRITICAL risk warnings from impact analysis.
- NEVER rename symbols with find-and-replace — use `gitnexus_rename` which understands the call graph.
- NEVER commit changes without running `gitnexus_detect_changes()` to check affected scope.

## Resources

| Resource | Use for |
|----------|---------|
| `gitnexus://repo/us-stock-linebot/context` | Codebase overview, check index freshness |
| `gitnexus://repo/us-stock-linebot/clusters` | All functional areas |
| `gitnexus://repo/us-stock-linebot/processes` | All execution flows |
| `gitnexus://repo/us-stock-linebot/process/{name}` | Step-by-step execution trace |

## CLI

| Task | Read this skill file |
|------|---------------------|
| Understand architecture / "How does X work?" | `.claude/skills/gitnexus/gitnexus-exploring/SKILL.md` |
| Blast radius / "What breaks if I change X?" | `.claude/skills/gitnexus/gitnexus-impact-analysis/SKILL.md` |
| Trace bugs / "Why is X failing?" | `.claude/skills/gitnexus/gitnexus-debugging/SKILL.md` |
| Rename / extract / split / refactor | `.claude/skills/gitnexus/gitnexus-refactoring/SKILL.md` |
| Tools, resources, schema reference | `.claude/skills/gitnexus/gitnexus-guide/SKILL.md` |
| Index, status, clean, wiki CLI commands | `.claude/skills/gitnexus/gitnexus-cli/SKILL.md` |

<!-- gitnexus:end -->
