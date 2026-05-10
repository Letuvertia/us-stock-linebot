# US Stock Analysis LINE Bot

GAS-based LINE bot for US stock analysis. Powered by Gemini 3 Flash, Yahoo Finance, and RSS news feeds.

## Features

- **Module A** — Daily stock scanner: ranks NASDAQ/NYSE stocks by upside potential (pre-market & post-market)
- **Module B** — News-driven RAG analysis: collects RSS news, tags against watchlist, generates academic-style analysis in Traditional Chinese
- **Module C** — Interactive assistant: @mention the bot in LINE group for real-time chat

## Setup

### 1. Create GAS Project

```bash
npm install
clasp login
clasp create --type webapp --title "US Stock LINE Bot"
```

Update `.clasp.json` with the generated `scriptId`.

### 2. Configure API Keys

Run `setAPIKeys()` in the GAS editor, then update values in **Project Settings > Script Properties**:

| Key | Description |
|-----|-------------|
| `LINE_CHANNEL_ACCESS_TOKEN` | LINE Messaging API channel access token |
| `LINE_GROUP_ID` | Target LINE group ID for push messages |
| `GEMINI_API_KEY` | Google AI Studio API key |

### 3. Initialize Sheets

Run `initializeAllSheets()` in the GAS editor. This creates:
- **NewsStore** — RSS news articles with ticker tags
- **UserConfig** — Watchlist and settings (JSON)
- **SystemLogs** — Execution logs
- **StockUniverse** — ~150 pre-seeded tickers for scanning

### 4. Deploy & Install Triggers

```bash
clasp push
```

Run `installTriggers()` in the GAS editor to set up:
- Pre-market scan at 20:30 (Taiwan time)
- Post-market scan at 05:30
- News collection every 6 hours
- News analysis at 18:00
- Old news cleanup at 03:00

### 5. Set Up LINE Webhook

Deploy as web app in GAS, then set the webhook URL in [LINE Developer Console](https://developers.line.biz/).

### 6. CI/CD (Optional)

Add `CLASP_TOKEN` (contents of `~/.clasprc.json`) to GitHub Secrets. Pushes to `main` auto-deploy via GitHub Actions.
