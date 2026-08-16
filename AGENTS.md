# US Stock LINE Bot

## System Overview

The system is a hybrid application:
- **GAS TypeScript Bot (`src/`)**: Handles LINE webhook interactions, command parsing, and scheduled reporting. GAS operates in **read-only** mode against Google Sheets.
- **Python Collectors (`scripts/`)**: Gathers market quotes, analyst estimates, news feeds, and podcasts via GitHub Actions and local cron jobs. Python collectors own all writes to Google Sheets.
- **Google Sheets**: Serves as the central shared database (historical sheets, metadata, and cache).

See [README.md](file:///home/letuvertia/us-stock-linebot/README.md) for full architecture diagrams, command syntax, database schemas, repository layout, and feature specifications.

---

## Develop and Test

- **Branching**: Always branch off latest `main` (`feat/<topic>`, `fix/<topic>`, `chore/<topic>`). Never commit directly to `main`.
- **Pull Requests**:
  - Title format: `[<base-branch>] <type>: <subject>` (e.g. `[main] fix: install triggers via doGet`).
  - Description: Include a concise Summary and a clear Test Plan.
- **Strict Single-Commit Rule**: Each PR must end with **exactly one commit**. Squash iterations using `git cpf` (`git commit --amend --no-edit && git push --force-with-lease origin $(git rev-parse --abbrev-ref HEAD)`).

### Github Python Environment (`scripts/`)
All secrets and credentials required by Python collector scripts (Google service accounts, spreadsheet IDs, and API keys) are already pre-configured both in GitHub Actions Secrets for scheduled workflows and locally in `.secrets/` and `.env` on this development machine. You can run collectors directly without extra virtual environment or credential setup.

### GAS TypeScript Environment (`src/`)
CI/CD (`.github/workflows/cicd.yaml`) runs automatically on push to all branches (builds TypeScript, runs `clasp push`, updates the deployment via `clasp deploy --deploymentId $GAS_DEPLOYMENT_ID`, and reconciles triggers). There is no need for local clasp testing; pushing to any feature branch automatically deploys a preview build (`<latest-tag>-<short-sha>`) to the existing deployment URL so you can test changes directly against the webhook endpoint.

To test the GAS webhook handler without sending messages from a physical LINE app, send a simulated POST request:
```bash
curl -s -L "https://script.google.com/macros/s/<DEPLOYMENT_ID>/exec" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {
        "type": "message",
        "replyToken": "test-reply-token",
        "source": { "type": "user", "userId": "test-user-id" },
        "message": { "type": "text", "text": "皮皮 目標價 NVDA" }
      }
    ]
  }'
```

---

## Debug

### GAS / Apps Script Logs
To inspect real-time execution logs, incoming user chat queries, uncaught exceptions, and webhook traces directly:
```bash
cd src && npx clasp logs
```
*Options: `--watch` to stream live logs, `--json` for raw JSON output.* Logs include parsed user messages (`[INFO] handleChatWebhook: Received query: ...`), dispatch actions, trigger events, and application-level errors (which are also appended to the `Logs` tab in `LINEBOT_LOGS_SPREADSHEET_ID`).

### Google Sheets
Google Sheets serves as the database for market quotes, news, podcast transcripts, user holdings, and logs. Access sheets in Python using `gspread` with service account credentials configured in `DATA_CREDS_FILE` / `NEWS_CREDS_FILE`.

- **Metadata Spreadsheet (`US_STOCK_SPREADSHEET_ID`)**:
  - `StockUniverse`: Master list of tracked tickers, company names, and the **latest** market snapshot (price, high, low, target price, P/E, etc.) along with the last update timestamps written by Python collectors (GitHub Actions or local cronjobs). Use `StockUniverse` if you only need to verify the latest metrics or check if collector updates ran on time (via columns `Finnhub_Updated_At`, `FMP_Updated_At`, `MW_Updated_At`, `YF_Updated_At`).
  - `StockSheetIDs`: Maps stock tickers (e.g. `AAPL`, `NVDA`) to their dedicated spreadsheet IDs. Open individual stock sheets only when you need **historical** time-series data (`Daily` tab).
  - `NewsSheetIDs`: Maps news sources (e.g. `CNBC`, `Reuters`) to their news archive spreadsheet IDs.
  - `PodcastSheetIDs`: Maps podcast shows (e.g. `Gooaye`, `RichWomen`) to their transcript/summary spreadsheet IDs.

- **User Config Spreadsheet (`USER_CONFIG_SPREADSHEET_ID`)**:
  - Stores user portfolio allocations (`UserHoldings`), news keyword preferences (`News Keywords`), and customized industry classifications (`Industry Category`).

