interface IndustryStock {
  ticker: string;
  name: string;
  forwardPE: number;
}

interface IndustryGroup {
  category: string;
  subCategory: string;
  stocks: IndustryStock[];
}

function _loadIndustryCategories(): Array<{ ticker: string; category: string; subCategory: string }> {
  const id = getScriptProperty(PROP_KEYS.USER_CONFIG_SPREADSHEET_ID);
  const ss = SpreadsheetApp.openById(id);
  const sheet = ss.getSheetByName('Industry Category');
  if (!sheet || sheet.getLastRow() <= 1) return [];

  const all = sheet.getRange(1, 1, sheet.getLastRow(), sheet.getLastColumn()).getValues();
  const headers = (all[0] as unknown[]).map(h => String(h).trim());

  const tickerIdx = headers.indexOf('Ticker');
  const catIdx = headers.indexOf('Customized Industry Category');
  const subCatIdx = headers.indexOf('Customized Industry SubCategory');

  if (tickerIdx < 0 || catIdx < 0 || subCatIdx < 0) {
    logWarn('_loadIndustryCategories', `Expected headers not found. Got: ${headers.slice(0, 8).join(', ')}`);
    return [];
  }

  const result: Array<{ ticker: string; category: string; subCategory: string }> = [];
  for (let i = 1; i < all.length; i++) {
    const row = all[i] as unknown[];
    const ticker = String(row[tickerIdx] || '').trim();
    const category = String(row[catIdx] || '').trim();
    const subCategory = String(row[subCatIdx] || '').trim();
    if (ticker && category && subCategory) {
      result.push({ ticker, category, subCategory });
    }
  }
  return result;
}

function _buildForwardPeMap(): Map<string, { name: string; forwardPE: number | null }> {
  const rows = getAllRows(SHEET_NAMES.STOCK_UNIVERSE);
  const map = new Map<string, { name: string; forwardPE: number | null }>();
  for (const row of rows) {
    const ticker = String((row as unknown[])[0] || '').trim();
    if (!ticker) continue;
    map.set(ticker, {
      name: String((row as unknown[])[COL.NAME] || ''),
      forwardPE: _num(row as unknown[], COL.FWD_PE),
    });
  }
  return map;
}

function formatIndustryPeRanking(): string {
  const entries = _loadIndustryCategories();
  if (entries.length === 0) return '(No industry category data found)';

  const stockMap = _buildForwardPeMap();

  const groupMap = new Map<string, IndustryGroup>();
  for (const entry of entries) {
    const key = `${entry.category}\x00${entry.subCategory}`;
    if (!groupMap.has(key)) {
      groupMap.set(key, { category: entry.category, subCategory: entry.subCategory, stocks: [] });
    }
    const info = stockMap.get(entry.ticker);
    if (info && info.forwardPE !== null && info.forwardPE > 0) {
      groupMap.get(key)!.stocks.push({ ticker: entry.ticker, name: info.name, forwardPE: info.forwardPE });
    }
  }

  const groups = [...groupMap.values()]
    .filter(g => g.stocks.length > 0)
    .sort((a, b) => {
      const c = a.category.localeCompare(b.category);
      return c !== 0 ? c : a.subCategory.localeCompare(b.subCategory);
    });

  if (groups.length === 0) return '(No Forward P/E data available)';

  const date = formatDateTW(new Date());
  let msg = `📊 產業 Forward P/E 排行 (${date})\n`;
  msg += `排序: 各群組由低至高\n\n`;

  for (const group of groups) {
    group.stocks.sort((a, b) => a.forwardPE - b.forwardPE);

    msg += `${group.category}/${group.subCategory}\n`;

    // Build labels and compute max width for alignment within this group
    const labels = group.stocks.map(s => {
      const shortName = s.name.length > 16 ? s.name.slice(0, 15) + '…' : s.name;
      return `${s.ticker}  ${shortName}`;
    });
    const maxLen = Math.max(...labels.map(l => l.length));

    for (let i = 0; i < group.stocks.length; i++) {
      const pe = group.stocks[i].forwardPE.toFixed(1);
      msg += `${labels[i].padEnd(maxLen)}  ${pe}\n`;
    }

    msg += `\n`;
  }

  return msg.trimEnd();
}

function executeIndustryPeReport(): void {
  const fnName = 'executeIndustryPeReport';
  logInfo(fnName, 'Starting industry P/E report');

  const message = withErrorHandling(fnName, () => formatIndustryPeRanking());
  if (!message) return;

  sendPushMessage(message);
  logInfo(fnName, 'Industry P/E report pushed to LINE');
}
