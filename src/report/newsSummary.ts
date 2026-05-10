// columns: ID(0), Date(1), TickerTags(2), Title(3), Content(4), URL(5), Processed_At(6), Summary(7)
const NEWS_MAX_RESULTS = 10;
const NEWS_LOOKBACK_DAYS = 7;

// ===== LINE chat query: news by ticker =====

function queryNewsByTicker(query: string): string | null {
  const fnName = 'queryNewsByTicker';

  const kwMap = _loadTickerKeywordsMap();
  const queryLower = query.toLowerCase();
  const sortedKws = [...kwMap.keys()].sort((a, b) => b.length - a.length);

  let foundTicker: string | null = null;
  for (const kw of sortedKws) {
    const isAscii = /^[\x00-\x7F]+$/.test(kw);
    const matched = isAscii
      ? new RegExp(`(?<![A-Za-z])${kw.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}(?![A-Za-z])`, 'i').test(query)
      : queryLower.includes(kw.toLowerCase());
    if (matched) {
      foundTicker = kwMap.get(kw)!;
      break;
    }
  }

  if (!foundTicker) return null;

  const sheetId = getNewsSheetId('CNBC');
  const ss = SpreadsheetApp.openById(sheetId);
  const sheet = ss.getSheets()[0];
  const rows = sheet.getDataRange().getValues() as string[][];

  const cutoff = new Date(Date.now() - NEWS_LOOKBACK_DAYS * 86400000);

  const matches = rows.slice(1).filter(r => {
    const tags = String(r[2] || '');
    const dateStr = String(r[1] || '');
    if (!tags || !dateStr) return false;
    const tagList = tags.split(',').map(t => t.trim().toUpperCase());
    if (!tagList.includes(foundTicker!)) return false;
    const d = new Date(dateStr);
    return !isNaN(d.getTime()) && d >= cutoff;
  });

  if (matches.length === 0) {
    return `📰 ${foundTicker} 近 ${NEWS_LOOKBACK_DAYS} 天無相關新聞`;
  }

  matches.sort((a, b) => new Date(String(b[1])).getTime() - new Date(String(a[1])).getTime());
  const top = matches.slice(0, NEWS_MAX_RESULTS);

  let msg = `📰 ${foundTicker} 相關新聞 (近 ${NEWS_LOOKBACK_DAYS} 天，共 ${matches.length} 則)\n`;
  msg += `──────────────\n\n`;

  msg += top.map(r => {
    const date = String(r[1] || '').slice(0, 10);
    const title = String(r[3] || '').trim();
    const url = String(r[5] || '').trim();
    const summary = String(r[7] || '').trim();
    let entry = `▸ [${date}] ${title}`;
    if (url) entry += `\n${url}`;
    if (summary) entry += `\n${_injectPrices(summary, _buildStockMap())}`;
    return entry;
  }).join('\n\n');

  if (matches.length > NEWS_MAX_RESULTS) {
    msg += `\n\n（還有 ${matches.length - NEWS_MAX_RESULTS} 則未顯示）`;
  }

  logInfo(fnName, `${foundTicker}: ${matches.length} news hits, showing ${top.length}`);
  return msg;
}

// ===== Daily batch report (legacy; replaced by real-time push below) =====

function executeDailyNewsReport(): void {
  const fnName = 'executeDailyNewsReport';

  const sheetId = getNewsSheetId('CNBC');
  const ss = SpreadsheetApp.openById(sheetId);
  const sheet = ss.getSheets()[0];
  const rows = sheet.getDataRange().getValues() as string[][];

  const yesterday = Utilities.formatDate(
    new Date(Date.now() - 86400000),
    TIMEZONE,
    'yyyy-MM-dd',
  );

  const articles = rows.slice(1).filter(r => {
    const date = String(r[1] || '');
    return date.startsWith(yesterday);
  });

  logInfo(fnName, `Found ${articles.length} CNBC articles for ${yesterday}`);

  if (articles.length === 0) {
    sendPushMessage(`📰 CNBC 昨日新聞 (${yesterday})\n\n（無新聞）`);
    return;
  }

  const stockMap = _buildStockMap();
  const header = `📰 CNBC 昨日新聞 (${yesterday})\n共 ${articles.length} 則\n──────────────\n\n`;

  const lines = articles.map(r => {
    const title = String(r[3] || '').trim();
    const url = String(r[5] || '').trim();
    const summary = String(r[7] || '').trim();
    let entry = `▸ ${title}`;
    if (url) entry += `\n${url}`;
    if (summary) entry += `\n${_injectPrices(summary, stockMap)}`;
    return entry;
  });

  const full = header + lines.join('\n\n');
  const chunks = splitLongMessage(full);
  const BATCH = 5;
  for (let i = 0; i < chunks.length; i += BATCH) {
    const batch = chunks.slice(i, i + BATCH).join('\n');
    sendPushMessage(batch);
    if (i + BATCH < chunks.length) {
      Utilities.sleep(500);
    }
  }
}

// ===== Real-time push (called from doPost when Python summarizer notifies) =====

function handleSummariesUpdated(ids: string[]): void {
  const fnName = 'handleSummariesUpdated';
  if (ids.length === 0) return;

  const sheetId = getNewsSheetId('CNBC');
  const ss = SpreadsheetApp.openById(sheetId);
  const sheet = ss.getSheets()[0];
  const rows = sheet.getDataRange().getValues() as string[][];

  const idSet = new Set(ids.map(String));
  const matches = rows.slice(1).filter(r => idSet.has(String(r[0] || '')));

  logInfo(fnName, `Received ${ids.length} IDs, found ${matches.length} rows`);
  if (matches.length === 0) return;

  const stockMap = _buildStockMap();
  const timeLabel = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy/M/d HH:mm');
  const header = `📰 新聞摘要速報 (${timeLabel}，${matches.length} 則)\n──────────────\n`;

  const lines = matches.map(r => {
    const date = String(r[1] || '').slice(0, 10);
    const title = String(r[3] || '').trim();
    const url = String(r[5] || '').trim();
    const summary = String(r[7] || '').trim();
    const monthDay = date.length >= 10 ? `${parseInt(date.slice(5, 7))}/${parseInt(date.slice(8, 10))}` : date;
    let entry = `▸ (${monthDay}) ${title}`;
    if (url) entry += `\n${url}`;
    if (summary) entry += `\n${_injectPrices(summary, stockMap)}`;
    return entry;
  });

  sendPushMessage(header + lines.join('\n\n'));
}

// ===== Helpers =====

function _buildStockMap(): Map<string, StockCandidate> {
  const map = new Map<string, StockCandidate>();
  loadStocksFromSheet().forEach(s => map.set(s.ticker, s));
  return map;
}

// Inject live prices into summary lines like "利多: NVDA - reason" → "📈利多: NVDA ($207.83, +1.92%, 目標$267.50) - reason"
function _injectPrices(summary: string, stockMap: Map<string, StockCandidate>): string {
  return summary.replace(
    /^(利多|利空|中立): ([A-Z]{1,5})(?: - (.*))?$/gm,
    (_, impact, ticker, reason) => {
      const s = stockMap.get(ticker);
      let priceLabel = '';
      if (s) {
        const parts: string[] = [];
        if (s.currentPrice) parts.push(`$${s.currentPrice.toFixed(2)}`);
        if (s.changePct != null) {
          const sign = s.changePct >= 0 ? '+' : '';
          parts.push(`${sign}${s.changePct.toFixed(2)}%`);
        }
        if (s.mwTargetMedian) parts.push(`目標$${s.mwTargetMedian.toFixed(2)}`);
        if (parts.length) priceLabel = ` (${parts.join(', ')})`;
      }
      const emoji = impact === '利多' ? '📈' : impact === '利空' ? '📉' : '🔲';
      const reasonPart = reason ? ` - ${reason}` : '';
      return `${emoji}${impact}: ${ticker}${priceLabel}${reasonPart}`;
    },
  );
}
