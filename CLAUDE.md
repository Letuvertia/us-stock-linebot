# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

Repository root holds only docs (`README.md`, `CLAUDE.md`) and two source folders:

- `src/` — GAS TypeScript bot (LINE webhook handlers, scheduled reports). All build files live here: `package.json`, `tsconfig.json`, `.clasp.json`, `appsscript.json`. Run `npm` commands from `src/`.
- `scripts/` — Python collectors. Two main subfolders: `market_data/` (price/quote collectors) and `market_news/` (CNBC/Reuters news + summarizer).

## Developer workflow

**Every change goes through a branch + PR — never commit directly to `main`.**

1. Branch off `main`: `feat/<topic>` for new features, `fix/<topic>` for bug fixes
2. Make commits as you work
3. Open a PR with `gh pr create` and write a clear description (Summary + Test plan). Update the description as scope evolves. Title format: `[<base-branch>] <type>: <subject>` (e.g. `[main] fix: install triggers via doGet`) so the merge target is visible at a glance.
4. **Each PR ends with exactly one commit.** As you iterate, squash with `git cpf` (alias for `commit --amend --no-edit && push --force-with-lease`). Never let a PR accumulate fix-up commits.
5. Merging the PR triggers the CI/CD pipeline on `main`, which auto-bumps the patch version and deploys.

The `git cpf` alias is configured globally:
```bash
git config --global alias.cpf '!git commit --amend --no-edit && git push --force-with-lease origin $(git rev-parse --abbrev-ref HEAD)'
```

## Build & Deploy

```bash
cd src && npm run build    # Compile TypeScript to src/dist/, copy appsscript.json
```

**To deploy GAS changes: open a PR and merge into `main`.** `cicd.yaml` runs from `src/`: build → push → deploy (updates the existing deployment ID, so the LINE webhook URL stays stable) → install triggers. Do NOT run `npm run push` manually.

Versioning:
- Push to `main` (after PR merge): patch tag bumped (e.g. `0.0.0` → `0.0.1`), deploy description shows new tag
- Push to other branches: deploy description shows `<latest-tag>-<short-sha>` (preview deploy on the same script)
- Manual `release.yaml` (workflow_dispatch with version input): tags + creates a GitHub release + deploys with that exact version

Trigger reconciliation runs `clasp run installTriggers` against the API executable deployment (separate from the web app deployment). Requires Apps Script API enabled at <https://script.google.com/home/usersettings> and an API executable deployment whose ID is stored in the `GAS_API_DEPLOYMENT_ID` secret.

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

Compiles with `module: "None"` — all functions share global scope (GAS requirement). No imports/exports. GAS is **read-only** from Sheets; Python collectors own all writes.

- **`main/localWebhook.ts`** — `doPost()` (GAS-reserved entry); thin shim that delegates to `handleLocalWebhook()`, which dispatches `summaries_updated` events to `handleSummariesUpdated` and falls through to `handleChatWebhook` for LINE traffic
- **`main/chatWebhook.ts`** — `handleChatWebhook()`: parses LINE webhook, detects @mentions, dispatches chat commands
- **`main/trigger.ts`** — Trigger handlers (`runPortfolioTWClose`, `runPortfolioUSClose`) and declarative `_triggerSpecs()` list. `installTriggers()` wipes-and-reinstalls; CI runs it via `npm run install-triggers` after each push
- **`config/config.ts`** — LINE API URLs, sheet names, script property keys, TIMEZONE, TOP_STOCKS_COUNT
- **`report/portfolio.ts`** — Portfolio P&L report (台股收盤 / 美股收盤), reads UserHoldings, fetches yfinance prices via UrlFetchApp
- **`report/newsSummary.ts`** — Merged news module: `queryNewsByTicker` (LINE chat query), `executeDailyNewsReport` (legacy daily batch, unscheduled), `handleSummariesUpdated` (real-time push triggered by Python summarizer), `_injectPrices`
- **`report/targetPriceRank.ts`** — Stock ranking: `loadStocksFromSheet()`, `rankStocks()`, `formatStockRanking()`, `executeStockScan()`
- **`report/industryPeRank.ts`** — Industry PE multiples report
- **`util/`** — `line.ts` (LINE messaging), `sheets.ts` (read-only Sheets helpers), `helpers.ts` (retry, error wrapping), `logger.ts` (gas-logs writer)

### Python Scripts (`scripts/`)

All use Google service account auth. Timestamps use UTC+8. Scripts live in `scripts/market_data/` and `scripts/market_news/`.

**Market data collectors** (`scripts/market_data/`):

| Script | Schedule | What it does |
|---|---|---|
| `collect_finnhub.py` | Hourly (skip 13:00-18:00 UTC+8) | Quote, ratings, metrics → per-stock sheet + StockUniverse |
| `collect_yfinance.py` | Daily 15:00 UTC+8 | OHLCV → per-stock sheet + StockUniverse |
| `collect_fmp.py` | Daily 14:00 UTC+8 | Price targets → per-stock sheet + StockUniverse |
| `collect_marketwatch.py` | Local cron 13:00/14:30/16:00/17:30 UTC+8 | Analyst estimates via nodriver. Resumes via `.mw_progress.json` |

**Market news collectors** (`scripts/market_news/`):

| Script | Schedule | What it does |
|---|---|---|
| `collect_cnbc.py` | Hourly | CNBC RSS → per-source news sheet |
| `collect_reuters.py` | Hourly | Reuters RSS → per-source news sheet |
| `summarize_cnbc.py` | Local cron hourly :05 | LLM summarization → notify GAS webhook → real-time LINE push |
| `cleanup_news.py` | Daily 2:47 AM | Deletes news rows older than 7 days |

**Shared modules**: `scripts/market_data/data_common.py` (Sheets helpers, stock sheet CRUD, universe batch writes), `scripts/market_news/news_common.py` (anti-bot headers, ticker keyword matching, article extraction)

### Google Sheets Architecture

Data lives in Google Drive folder `1kpHXJlv4Abb_S6J8vTSUv44FOQEzDPMu`:

- **`/stocks/`** — 500 individual spreadsheets named `{TICKER} - {Company Name}`, each with a `Daily` tab (one row per trading day, 170 columns from `Data Schema`)
- **Main spreadsheet** (`1e_FRJDfF6mwt3FWxMZDuyBKpHCiTFHhsGbppRFCvDXU`): `StockUniverse` (latest snapshot), `StockSheetIDs` (ticker→spreadsheet ID mapping), `Data Schema` (170-column header definitions)
- **News spreadsheets** — one per source (CNBC, Reuters), columns: ID, Date, TickerTags, Title, Content, URL, Processed_At
- **user-config** (`1rIVv2lZDrUT7bCO8iXzl5g5J_-BKA7RjusT64akZD0k`): `News Keywords` tab (ticker keywords for news tagging, col H), `Users` tab
- **gas-logs** — SystemLogs written by GAS

All collectors write to both per-stock sheets (historical) and StockUniverse (latest snapshot) using batch writes.

## Key Patterns

- **Retry with backoff**: `retryWithBackoff()` in GAS, `*_with_retry()` in Python — all external API calls use retry
- **Batch processing**: All collectors batch StockUniverse writes (20 tickers per batch via `batch_write_universe()`)
- **News tagging**: `extract_ticker_tags()` in `news_common.py` matches article text against user-config News Keywords tab (col H), not ticker symbols, to avoid false positives from short tickers (A, IT, ON)
- **MarketWatch anti-bot**: Uses `nodriver` (undetected headless Chrome) to bypass DataDome. Playwright/urllib get blocked. Progress saved to `.mw_progress.json` for resume. GitHub Actions IPs are blocked — runs via local Windows cron only (needs residential VPN)
- **News anti-bot**: Rotating User-Agents, shared cookie jar, random delays (1.5-4s) in `news_common.py`
- **Sheets quota**: 60 writes/min. Python scripts use batch writes or rate-limit pauses
- **Per-stock sheets**: `find_or_create_today_row()` + `write_stock_data()` in `data_common.py` — each day gets one row, keyed by US Eastern date

## Secrets & Config

**GAS Script Properties** (set in Apps Script editor): SPREADSHEET_ID, FINNHUB_API_KEY, LINE_CHANNEL_ACCESS_TOKEN, LINE_CHANNEL_SECRET, LINE_GROUP_ID, OPENAI_API_KEY

**GitHub Actions Secrets**: GOOGLE_SERVICE_ACCOUNT_KEY, SPREADSHEET_ID, FMP_API_KEY, FINNHUB_API_KEY, CLASP_TOKEN

## Cron Schedules (all times UTC+8)

- Finnhub: hourly at :15 (skip 14:00-16:00 to avoid Sheets quota contention)
- CNBC news: hourly at :00
- Reuters news: hourly at :30
- FMP targets: 14:00 daily
- yfinance: 15:00 daily
- MarketWatch: local Windows cron (needs VPN, not GitHub Actions)
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
