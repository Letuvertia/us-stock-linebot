// --- API Endpoints ---

const FINNHUB_BASE_URL = 'https://finnhub.io/api/v1';

const OPENAI_API_BASE = 'https://api.openai.com/v1/chat/completions';

const LINE_PUSH_URL = 'https://api.line.me/v2/bot/message/push';
const LINE_REPLY_URL = 'https://api.line.me/v2/bot/message/reply';

// --- Sheet Configuration ---

const SHEET_NAMES = {
  NEWS_STORE: 'NewsStore',
  USER_CONFIG: 'UserConfig',
  SYSTEM_LOGS: 'SystemLogs',
  STOCK_UNIVERSE: 'StockUniverse',
} as const;

const SHEET_HEADERS: Record<string, string[]> = {
  [SHEET_NAMES.NEWS_STORE]: ['ID', 'Date', 'TickerTags', 'Title', 'Content', 'URL', 'Processed_At'],
  [SHEET_NAMES.USER_CONFIG]: ['Config_Key', 'Config_Value'],
  [SHEET_NAMES.SYSTEM_LOGS]: ['Timestamp', 'Level', 'Function', 'Message'],
  [SHEET_NAMES.STOCK_UNIVERSE]: [
    'Ticker', 'Exchange', 'Name',
    'Finnhub_Current_Price', 'Finnhub_Change_Pct', 'Finnhub_Open', 'Finnhub_High', 'Finnhub_Low', 'Finnhub_Prev_Close',
    'Finnhub_52W_High', 'Finnhub_52W_Low', 'Finnhub_Dist_From_High_Pct',
    'Finnhub_StrongBuy', 'Finnhub_Buy', 'Finnhub_Hold', 'Finnhub_Sell', 'Finnhub_StrongSell', 'Finnhub_Rating_Score',
    'Finnhub_PE_TTM', 'Finnhub_Forward_PE', 'Finnhub_PEG', 'Finnhub_EPS_TTM', 'Finnhub_EPS_Growth_Pct',
    'Finnhub_Beta', 'Finnhub_Market_Cap_M', 'Finnhub_Dividend_Yield',
    'Finnhub_Operating_Margin', 'Finnhub_Net_Margin', 'Finnhub_ROE',
    'Finnhub_Industry', 'Finnhub_Updated_At',
    'FMP_Target_High', 'FMP_Target_Low', 'FMP_Target_Consensus', 'FMP_Target_Median', 'FMP_Upside_Pct', 'FMP_Updated_At',
    'MW_Target_High', 'MW_Target_Low', 'MW_Target_Median', 'MW_Target_Avg',
    'MW_Num_Ratings', 'MW_Upside_Pct',
    'MW_EPS_FY1_Avg', 'MW_EPS_FY2_Avg',
    'MW_EPS_LQ_Est', 'MW_EPS_LQ_Act', 'MW_EPS_LQ_Surprise',
    'MW_Updated_At',
    'Keywords',
  ],
};

// --- Script Property Keys ---

const PROP_KEYS = {
  SPREADSHEET_ID: 'SPREADSHEET_ID',
  FINNHUB_API_KEY: 'FINNHUB_API_KEY',
  LINE_CHANNEL_ACCESS_TOKEN: 'LINE_CHANNEL_ACCESS_TOKEN',
  LINE_CHANNEL_SECRET: 'LINE_CHANNEL_SECRET',
  LINE_GROUP_ID: 'LINE_GROUP_ID',
  OPENAI_API_KEY: 'OPENAI_API_KEY',
} as const;

// --- Constants ---

const TIMEZONE = 'Asia/Taipei';
const FINNHUB_DELAY_MS = 1100;  // Free tier: 60 calls/min
const OPENAI_DELAY_MS = 4000;
const MIN_VOLUME = 500000;
const TOP_STOCKS_COUNT = 20;
const NEWS_RETENTION_DAYS = 7;
