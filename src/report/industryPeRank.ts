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

function _buildIndustryMap(): Map<string, string> {
  const map = new Map<string, string>();
  try {
    const entries = _loadIndustryCategories();
    for (const e of entries) {
      map.set(e.ticker, `${e.category}/${e.subCategory}`);
    }
  } catch (_) {
    // USER_CONFIG_SPREADSHEET_ID not set — industry labels will show '-'
  }
  return map;
}

interface StockData {
  ticker: string;
  name: string;
  peTTM: number | null;
  forwardPE: number | null;
  peers: string[];
}

interface PeerRow {
  ticker: string;
  name: string;
  peTTM: number | null;
  forwardPE: number | null;
  isMain: boolean;
}

interface QualifyingGroup {
  mainTicker: string;
  rows: PeerRow[];
}

function _loadPeerStocksFromSheet(): Map<string, StockData> {
  const rows = getAllRows(SHEET_NAMES.STOCK_UNIVERSE);
  const map = new Map<string, StockData>();

  for (const row of rows) {
    const r = row as unknown[];
    const ticker = String(r[COL.TICKER] || '').trim();
    if (!ticker) continue;

    const peersRaw = String(r[COL.PEERS] || '').trim();
    const peers = peersRaw ? peersRaw.split(',').map(p => p.trim()).filter(Boolean) : [];

    map.set(ticker, {
      ticker,
      name: String(r[COL.NAME] || ''),
      peTTM: _num(r, COL.PE),
      forwardPE: _num(r, COL.FWD_PE),
      peers,
    });
  }

  return map;
}

function findPeerUndervalued(stockMap: Map<string, StockData>): QualifyingGroup[] {
  const universe = new Set(stockMap.keys());
  const results: QualifyingGroup[] = [];

  for (const [ticker, stock] of stockMap) {
    if (stock.peTTM === null || stock.peTTM <= 0) continue;

    const inUniPeers = stock.peers
      .filter((p, i, arr) => p !== ticker && universe.has(p) && arr.indexOf(p) === i);

    const validPePeers = inUniPeers
      .map(p => stockMap.get(p)!)
      .filter(p => p.peTTM !== null && p.peTTM > 0);

    if (validPePeers.length < 2) continue;

    const group = [...validPePeers, stock].sort((a, b) => (a.peTTM ?? 0) - (b.peTTM ?? 0));
    const rank = group.findIndex(s => s.ticker === ticker) + 1;
    const threshold = Math.ceil(0.30 * group.length);

    if (rank > threshold) continue;

    const validRows: PeerRow[] = group.map(s => ({
      ticker: s.ticker,
      name: s.name,
      peTTM: s.peTTM,
      forwardPE: s.forwardPE,
      isMain: s.ticker === ticker,
    }));

    const noPeRows: PeerRow[] = inUniPeers
      .map(p => stockMap.get(p)!)
      .filter(p => p.peTTM === null || p.peTTM <= 0)
      .map(p => ({ ticker: p.ticker, name: p.name, peTTM: null, forwardPE: p.forwardPE, isMain: false }));

    results.push({ mainTicker: ticker, rows: [...validRows, ...noPeRows] });
  }

  results.sort((a, b) => {
    const aPe = stockMap.get(a.mainTicker)?.peTTM ?? Infinity;
    const bPe = stockMap.get(b.mainTicker)?.peTTM ?? Infinity;
    return aPe - bPe;
  });
  return results;
}

function _trunc(s: string, max: number): string {
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}

function formatPeerPeReport(groups: QualifyingGroup[], industryMap: Map<string, string>): string {
  const date = formatDateTW(new Date());
  const road = _pick(ROADS);
  const location = _pick(LOCATIONS);

  let msg = `皮皮在${road}的${location}找到了一份資料！\n`;
  msg += `──────────────\n\n`;
  msg += `📊 同業低估 P/E 掃描 (${date})\n`;
  msg += `條件: P/E 在同業後 30%\n\n`;

  for (const group of groups) {
    msg += `▸ ${group.mainTicker}\n\n`;

    const maxLen = Math.max(...group.rows.map(r => r.ticker.length));
    const IND_WIDTH = 20;
    const PE_WIDTH = 5;

    for (const row of group.rows) {
      const prefix = row.isMain ? '★' : ' ';
      const t = row.ticker.padEnd(maxLen);
      const ind = _trunc(industryMap.get(row.ticker) || '-', IND_WIDTH).padEnd(IND_WIDTH);
      const pe = row.peTTM !== null ? row.peTTM.toFixed(1).padStart(PE_WIDTH) : '  N/A';
      const fwd = row.forwardPE !== null ? row.forwardPE.toFixed(1).padStart(PE_WIDTH) : '  N/A';
      msg += `${prefix}${t}  ${ind}  P/E:${pe}  FwP/E:${fwd}\n`;
    }

    msg += `\n`;
  }

  msg += `──────────────\n`;
  msg += `共 ${groups.length} 檔符合條件`;
  return msg;
}

function queryPeerPeByCategory(categoryQuery: string): string | null {
  let entries: Array<{ ticker: string; category: string; subCategory: string }> = [];
  try {
    entries = _loadIndustryCategories();
  } catch (_) {
    return '(無法讀取產業分類資料)';
  }
  if (entries.length === 0) return '(找不到產業分類資料)';

  // Find matching label
  const labels = new Map<string, 'category' | 'subCategory'>();
  for (const e of entries) {
    if (e.category) labels.set(e.category, 'category');
    if (e.subCategory) labels.set(e.subCategory, 'subCategory');
  }

  let matchedLabel: string | null = null;
  let matchedField: 'category' | 'subCategory' | null = null;
  for (const [label, field] of labels) {
    if (categoryQuery.includes(label)) {
      matchedLabel = label;
      matchedField = field;
      break;
    }
  }

  if (!matchedLabel || !matchedField) return null;

  const industryTickers = new Set<string>();
  for (const e of entries) {
    if (matchedField === 'category' && e.category === matchedLabel) industryTickers.add(e.ticker);
    if (matchedField === 'subCategory' && e.subCategory === matchedLabel) industryTickers.add(e.ticker);
  }

  const stockMap = _loadPeerStocksFromSheet();
  const industryMap = _buildIndustryMap();
  const allGroups = findPeerUndervalued(stockMap);

  const filtered = allGroups.filter(g => industryTickers.has(g.mainTicker));
  if (filtered.length === 0) return `(${matchedLabel} 目前無符合 P/E 後 30% 的股票)`;

  // Reuse formatter but override header label
  const date = formatDateTW(new Date());
  const road = _pick(ROADS);
  const location = _pick(LOCATIONS);

  let msg = `皮皮在${road}的${location}找到了一份資料！\n`;
  msg += `──────────────\n\n`;
  msg += `📊 ${matchedLabel} 低估 P/E 掃描 (${date})\n`;
  msg += `條件: P/E 在同業後 30%\n\n`;

  for (const group of filtered) {
    msg += `▸ ${group.mainTicker}\n\n`;

    const maxLen = Math.max(...group.rows.map(r => r.ticker.length));
    const IND_WIDTH = 20;
    const PE_WIDTH = 5;

    for (const row of group.rows) {
      const prefix = row.isMain ? '★' : ' ';
      const t = row.ticker.padEnd(maxLen);
      const ind = _trunc(industryMap.get(row.ticker) || '-', IND_WIDTH).padEnd(IND_WIDTH);
      const pe = row.peTTM !== null ? row.peTTM.toFixed(1).padStart(PE_WIDTH) : '  N/A';
      const fwd = row.forwardPE !== null ? row.forwardPE.toFixed(1).padStart(PE_WIDTH) : '  N/A';
      msg += `${prefix}${t}  ${ind}  P/E:${pe}  FwP/E:${fwd}\n`;
    }

    msg += `\n`;
  }

  msg += `──────────────\n`;
  msg += `共 ${filtered.length} 檔符合條件`;
  return msg;
}

function executeIndustryPeReport(): void {
  const fnName = 'executeIndustryPeReport';
  logInfo(fnName, 'Starting peer P/E undervaluation scan');

  const stockMap = _loadPeerStocksFromSheet();
  if (stockMap.size === 0) {
    logWarn(fnName, 'No stock data in StockUniverse');
    return;
  }

  const industryMap = _buildIndustryMap();
  const groups = findPeerUndervalued(stockMap);

  if (groups.length === 0) {
    logWarn(fnName, 'No tickers qualified (bottom 30% P/E vs peers)');
    return;
  }

  logInfo(fnName, `${groups.length} tickers qualified`);
  const message = formatPeerPeReport(groups, industryMap);
  sendPushMessage(message);
  logInfo(fnName, 'Peer P/E report pushed to LINE');
}
