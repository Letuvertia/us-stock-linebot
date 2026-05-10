"""Centralized config for all Python scripts.

Single source of truth for required env vars and tuning constants.

Required env vars are checked LAZILY — they raise only when imported. So
data-only collectors don't need to set news-only env vars (and vice versa).
A missing env var fails at the script's import statement with a clear message,
not silently mid-run with a vague auth error.

Required env vars (raise on import if missing):
- DATA_CREDS_FILE: service account JSON path for metadata + per-stock spreadsheets
- NEWS_CREDS_FILE: service account JSON path for news spreadsheets
- US_STOCK_SPREADSHEET_ID: metadata spreadsheet (StockUniverse, NewsSheetIDs, Config)
- USER_CONFIG_SPREADSHEET_ID: user-config spreadsheet (industry tags, watchlist)

API keys (consumed directly by collectors via os.environ['NAME']):
- FMP_API_KEY (comma-separated)
- FINNHUB_API_KEY (comma-separated)

Tuning constants below have no env override — edit this file to change them.
"""
import os


def _load_dotenv() -> None:
    """Best-effort .env loader for local manual runs.

    Real env (set by cron or the calling shell) takes precedence via setdefault,
    so this only fills in unset vars. CI workers don't have a .env, the file
    simply isn't found, and this is a no-op.
    """
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(repo_root, '.env')
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())


_load_dotenv()

_REQUIRED_ENV_VARS = {
    'DATA_CREDS_FILE',
    'NEWS_CREDS_FILE',
    'US_STOCK_SPREADSHEET_ID',
    'USER_CONFIG_SPREADSHEET_ID',
}


def __getattr__(name):
    if name in _REQUIRED_ENV_VARS:
        val = os.environ.get(name)
        if not val:
            raise RuntimeError(f"Required env var '{name}' is not set")
        return val
    raise AttributeError(f"module 'config' has no attribute '{name}'")


# === Tuning constants ===

# Summarizer (scripts/market_news/summarize_cnbc.py)
OLLAMA_MODEL = 'qwen3.5:4b'
LOOKBACK_DAYS = 7

# MarketWatch collector (scripts/market_data/collect_marketwatch.py)
MW_REQUEST_DELAY_LO = 8.0
MW_REQUEST_DELAY_HI = 15.0
MW_BACKOFF_PAUSE = 600.0
MW_MAX_BLOCKS = 5
MW_MAX_RESTARTS = 3
