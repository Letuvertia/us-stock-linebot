// --- API Endpoints ---

const LINE_PUSH_URL = 'https://api.line.me/v2/bot/message/push';
const LINE_REPLY_URL = 'https://api.line.me/v2/bot/message/reply';

// --- Sheet Configuration ---

const SHEET_NAMES = {
  USER_CONFIG: 'UserConfig',
  STOCK_UNIVERSE: 'StockUniverse',
} as const;

// --- Script Property Keys ---

const PROP_KEYS = {
  SPREADSHEET_ID: 'SPREADSHEET_ID',
  LINEBOT_LOGS_SPREADSHEET_ID: 'LINEBOT_LOGS_SPREADSHEET_ID',
  USER_CONFIG_SPREADSHEET_ID: 'USER_CONFIG_SPREADSHEET_ID',
  LINE_CHANNEL_ACCESS_TOKEN: 'LINE_CHANNEL_ACCESS_TOKEN',
  LINE_CHANNEL_SECRET: 'LINE_CHANNEL_SECRET',
  LINE_GROUP_ID: 'LINE_GROUP_ID',
} as const;

// --- Constants ---

const TIMEZONE = 'Asia/Taipei';
const TOP_STOCKS_COUNT = 20;
