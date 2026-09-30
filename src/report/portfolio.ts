// Column layout (1-based):
//   A=Ticker  B=Exchange  C=Name  D=Type  E=Shares  F=AvgCost  G=TotalCost
//   H=CurrentPrice  I=CurrentROI  J=CurrentAsset(NTD/USD)  K=CurrentAsset(NTD)
//   L=LoanDate  M=AnnualRate  N=CurrentTotalAsset  O=CurrentTotalROI
// Row 2=Loan  Row 3=NTD cash  Row 4=USD cash  Row 5+=stocks

interface HoldingRow {
  sheetRow: number;
  type: string;
  ticker: string;
  exchange: string;
  name: string;
  shares: number;
  avgCost: number;
  totalCost: number;
  loanDate?: string;
  annualRate?: number;
}

// ── Price fetching via Yahoo Finance v8 ──────────────────────────────────────

function _yfPrice(symbol: string): { price: number; change: number; changePct: number } {
  // range=2d: chartPreviousClose = previous trading day's close (range=5d gives wrong value)
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=2d`;
  const resp = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'application/json',
    },
  });
  const code = resp.getResponseCode();
  if (code !== 200) {
    throw new Error(`YF fetch failed for ${symbol} with HTTP ${code}: ${resp.getContentText().slice(0, 100)}`);
  }
  const data = JSON.parse(resp.getContentText());
  const result = data?.chart?.result?.[0];
  if (!result) {
    throw new Error(`No result in YF payload for ${symbol}`);
  }
  const meta = result.meta;
  const price: number = meta.regularMarketPrice ?? 0;
  if (price <= 0) {
    throw new Error(`Invalid market price ${price} for ${symbol}`);
  }
  // Prefer the actual previous candle close over meta field
  const closes: number[] = (result.indicators?.quote?.[0]?.close ?? []).filter((c: number | null) => c != null);
  const prev: number = closes.length >= 2 ? closes[closes.length - 2] : (meta.chartPreviousClose ?? 0);
  const change = Math.round((price - prev) * 100) / 100;
  const changePct = prev > 0 ? (price - prev) / prev * 100 : 0;
  return { price, change, changePct };
}

function _fetchUsdNtd(): number {
  try {
    const { price } = retryWithBackoff(() => _yfPrice('USDTWD=X'), 3, 1000);
    return price > 0 ? price : 32.0;
  } catch (err) {
    logWarn('_fetchUsdNtd', `Failed to fetch USD/NTD rate: ${err instanceof Error ? err.message : String(err)}`);
    return 32.0;
  }
}

function _normalizeTwTicker(ticker: string): string {
  const trimmed = ticker.trim();
  if (/^\d+$/.test(trimmed)) {
    if (trimmed.length <= 2) return trimmed.padStart(4, '0');
    if (trimmed.length === 3) return trimmed.padStart(5, '0');
  }
  return trimmed;
}

// ── Taiwan & US Fee, Tax & Lot Tracking ─────────────────────────────────────

interface HoldingLot {
  shares: number;
  price: number;
  cost: number;
}

/**
 * 判斷台股標的是否為 ETF / ETN（享有 0.1% 優惠證交稅率）
 * 規則：凡台股代號以 00 或 02 開頭者皆為 ETF / ETN
 */
function _isTwEtf(ticker: string): boolean {
  const norm = _normalizeTwTicker(ticker);
  return /^00|^02/.test(norm);
}

/**
 * 取得台股賣出證券交易稅率 (ETF/ETN 0.1%, 一般個股 0.3%)
 */
function _twTaxRate(ticker: string): number {
  return _isTwEtf(ticker) ? 0.001 : 0.003;
}

interface TwPositionResult {
  grossAsset: number;
  netAsset: number;
  totalFee: number;
  totalTax: number;
  netPL: number;
  netRoi: number;
  buyPriceAvg: number;
}

/**
 * 計算台股賣出淨現值與損益（扣除國泰證券 2.8 折手續費 0.0399% 與證交稅，支援分批買進批次試算）
 */
function _calculateTwPosition(
  ticker: string,
  totalShares: number,
  price: number,
  totalCost: number,
  fallbackAvgCost: number,
  openLots?: HoldingLot[]
): TwPositionResult {
  const taxRate = _twTaxRate(ticker);
  const lotsShares = (openLots ?? []).reduce((sum, l) => sum + l.shares, 0);
  const useLots = openLots && openLots.length > 0 && Math.abs(lotsShares - totalShares) < 0.0001;

  if (useLots) {
    let grossAsset = 0;
    let totalFee = 0;
    let totalTax = 0;
    let netPL = 0;
    let totalTradeAmt = 0;
    for (const lot of openLots) {
      const lotGross = Math.round(lot.shares * price * 100) / 100;
      const lotFee = Math.max(1, Math.floor(lotGross * 0.001425 * 0.28));
      const lotTax = Math.floor(lotGross * taxRate);
      const lotNet = lotGross - lotFee - lotTax;
      grossAsset += lotGross;
      totalFee += lotFee;
      totalTax += lotTax;
      netPL += (lotNet - lot.cost);
      totalTradeAmt += (lot.shares * lot.price);
    }
    const netAsset = grossAsset - totalFee - totalTax;
    const netRoi = totalCost > 0 ? (netPL / totalCost) : 0;
    const buyPriceAvg = totalShares > 0 ? totalTradeAmt / totalShares : fallbackAvgCost;
    return { grossAsset, netAsset, totalFee, totalTax, netPL: Math.round(netPL), netRoi, buyPriceAvg };
  } else {
    const grossAsset = Math.round(totalShares * price * 100) / 100;
    const totalFee = Math.max(1, Math.floor(grossAsset * 0.001425 * 0.28));
    const totalTax = Math.floor(grossAsset * taxRate);
    const netAsset = grossAsset - totalFee - totalTax;
    const netPL = Math.round(netAsset - totalCost);
    const netRoi = totalCost > 0 ? (netPL / totalCost) : 0;
    return { grossAsset, netAsset, totalFee, totalTax, netPL, netRoi, buyPriceAvg: fallbackAvgCost };
  }
}

interface UsPositionResult {
  grossAsset: number;
  netAsset: number;
  totalFee: number;
  secFee: number;
  netPL: number;
  netRoi: number;
  buyPriceAvg: number;
}

/**
 * 計算美股賣出淨現值與損益（扣除國泰複委託 0.08% 手續費與 SEC 規費 0.00278%）
 */
function _calculateUsPosition(
  ticker: string,
  totalShares: number,
  price: number,
  totalCost: number,
  fallbackAvgCost: number,
  openLots?: HoldingLot[]
): UsPositionResult {
  const lotsShares = (openLots ?? []).reduce((sum, l) => sum + l.shares, 0);
  const useLots = openLots && openLots.length > 0 && Math.abs(lotsShares - totalShares) < 0.0001;

  const grossAsset = Math.round(totalShares * price * 100) / 100;
  const totalFee = Math.round(grossAsset * 0.0008 * 100) / 100;
  const secFee = Math.round(grossAsset * 0.0000278 * 100) / 100;
  const netAsset = Math.round((grossAsset - totalFee - secFee) * 100) / 100;
  const netPL = Math.round((netAsset - totalCost) * 100) / 100;
  const netRoi = totalCost > 0 ? (netPL / totalCost) : 0;

  let buyPriceAvg = fallbackAvgCost;
  if (useLots && openLots) {
    const totalTradeAmt = openLots.reduce((sum, l) => sum + l.shares * l.price, 0);
    buyPriceAvg = totalShares > 0 ? totalTradeAmt / totalShares : fallbackAvgCost;
  }

  return { grossAsset, netAsset, totalFee, secFee, netPL, netRoi, buyPriceAvg };
}

function _loadHoldingLots(): Map<string, HoldingLot[]> {
  const lotsMap = new Map<string, HoldingLot[]>();
  try {
    const ss = SpreadsheetApp.openById(getScriptProperty(PROP_KEYS.USER_CONFIG_SPREADSHEET_ID));
    const sheet = ss.getSheetByName('UserHoldingTransactions');
    if (!sheet) return lotsMap;
    const rows = sheet.getDataRange().getValues() as string[][];
    if (rows.length < 2) return lotsMap;

    const header = rows[0];
    const col = (name: string) => header.indexOf(name);
    const colEx = col('Exchange');
    const colTicker = col('Ticker');
    const colAction = col('Action');
    const colShares = col('Shares');
    const colPrice = col('Price');
    const colNetAmount = col('NetAmount');

    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      const ex = String(r[colEx] ?? '').trim().toUpperCase();
      const isTw = ex === 'TW' || ex === 'TWO';
      const isUs = ex === 'US';
      if (!isTw && !isUs) continue;

      const rawTicker = String(r[colTicker] ?? '').trim();
      if (!rawTicker) continue;
      const ticker = isTw ? _normalizeTwTicker(rawTicker) : rawTicker;
      const action = String(r[colAction] ?? '').trim();
      const shares = parseFloat(String(r[colShares] ?? '0')) || 0;
      const price = parseFloat(String(r[colPrice] ?? '0')) || 0;
      const netAmount = Math.abs(parseFloat(String(r[colNetAmount] ?? '0')) || 0);

      if (!lotsMap.has(ticker)) {
        lotsMap.set(ticker, []);
      }
      const lots = lotsMap.get(ticker)!;

      if (action === '買進') {
        lots.push({ shares, price, cost: netAmount });
      } else if (action === '賣出') {
        let rem = shares;
        while (rem > 0 && lots.length > 0) {
          if (lots[0].shares <= rem) {
            rem -= lots[0].shares;
            lots.shift();
          } else {
            const ratio = rem / lots[0].shares;
            lots[0].shares -= rem;
            lots[0].cost -= lots[0].cost * ratio;
            rem = 0;
          }
        }
      }
    }
  } catch (err) {
    logWarn('_loadHoldingLots', `Failed to load transaction lots: ${err instanceof Error ? err.message : String(err)}`);
  }
  return lotsMap;
}

// ── Loan interest ────────────────────────────────────────────────────────────

function _loanAccruedInterest(loanDate: string, principal: number, annualRate: number): number {
  try {
    const start = new Date(loanDate);
    const today = new Date();
    const days = Math.floor((today.getTime() - start.getTime()) / 86400000);
    return principal * annualRate * days / 365;
  } catch {
    return 0;
  }
}

// ── Formatting ───────────────────────────────────────────────────────────────

function _fmtNtd(v: number, decimals = 0): string {
  const abs = Math.abs(v);
  const sign = v < 0 ? '-' : '';
  return `${sign}NT$${abs.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals })}`;
}

function _fmtUsd(v: number): string {
  const abs = Math.abs(v);
  const sign = v < 0 ? '-' : '';
  return `${sign}US$${abs.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function _fmtNum(v: number, decimals = 0): string {
  return v.toLocaleString('en-US', { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function _pct(v: number, withSign = false): string {
  const sign = withSign && v > 0 ? '+' : '';
  return `${sign}${v.toFixed(2)}%`;
}

function _arrow(v: number): string {
  return v >= 0 ? '+' : '-';
}

// ── Sheet helpers ────────────────────────────────────────────────────────────

function _loadHoldings(): HoldingRow[] {
  const ss = SpreadsheetApp.openById(getScriptProperty(PROP_KEYS.USER_CONFIG_SPREADSHEET_ID));
  const sheet = ss.getSheetByName('UserHoldings');
  if (!sheet) throw new Error('UserHoldings tab not found');
  const rows = sheet.getDataRange().getValues() as string[][];
  if (rows.length < 2) return [];

  const header = rows[0];
  const col = (name: string) => header.indexOf(name);

  const getStr = (row: string[], name: string) => String(row[col(name)] ?? '');
  const getNum = (row: string[], name: string) => {
    const v = row[col(name)];
    const n = parseFloat(String(v));
    return isNaN(n) ? 0 : n;
  };

  return rows.slice(1).map((row, i) => ({
    sheetRow: i + 2,
    type: getStr(row, 'Type'),
    ticker: getStr(row, 'Ticker'),
    exchange: getStr(row, 'Exchange'),
    name: getStr(row, 'Name'),
    shares: getNum(row, 'Shares'),
    avgCost: getNum(row, 'AvgCost'),
    totalCost: getNum(row, 'TotalCost'),
    loanDate: getStr(row, 'LoanDate') || undefined,
    annualRate: getNum(row, 'AnnualRate') || undefined,
  }));
}

function _batchWrite(updates: { range: string; values: (string | number)[][] }[]): void {
  if (updates.length === 0) return;
  const ss = SpreadsheetApp.openById(getScriptProperty(PROP_KEYS.USER_CONFIG_SPREADSHEET_ID));
  const sheet = ss.getSheetByName('UserHoldings')!;
  for (const u of updates) {
    const m = u.range.match(/([A-Z]+)(\d+)(?::([A-Z]+)(\d+))?/);
    if (!m) continue;
    const startCol = _colIndex(m[1]);
    const startRow = parseInt(m[2]);
    sheet.getRange(startRow, startCol, u.values.length, u.values[0].length).setValues(u.values);
  }
}

function _colIndex(letter: string): number {
  let n = 0;
  for (let i = 0; i < letter.length; i++) n = n * 26 + (letter.charCodeAt(i) - 64);
  return n;
}

// ── Main report ──────────────────────────────────────────────────────────────

function executePortfolioReport(label?: string, replyToken?: string): void {
  const fnName = 'executePortfolioReport';

  if (!label) {
    const hour = parseInt(Utilities.formatDate(new Date(), TIMEZONE, 'HH'));
    label = (hour >= 12 && hour < 20) ? '台股收盤' : '美股收盤';
  }

  const rows = _loadHoldings();
  const holdingLotsMap = _loadHoldingLots();
  const usdNtd = _fetchUsdNtd();
  const now = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy-MM-dd HH:mm');

  const updates: { range: string; values: (string | number)[][] }[] = [];
  const DIVIDER = '──────────────';

  let stockNtd = 0;
  let cashNtd = 0;
  let loanRow: HoldingRow | null = null;
  let loanVal = 0;

  const twBlocks: string[][] = [];
  const usBlocks: string[][] = [];

  for (const row of rows) {
    const r = row.sheetRow;

    if (row.type === 'LOAN') {
      loanRow = row;
      const interest = _loanAccruedInterest(row.loanDate ?? '', row.shares, row.annualRate ?? 0);
      loanVal = -(row.shares + interest);
      updates.push({ range: `J${r}:K${r}`, values: [[Math.round(loanVal * 100) / 100, Math.round(loanVal * 100) / 100]] });
      continue;
    }

    if (row.type === 'CASH') {
      if (row.exchange === 'NTD') {
        const amt = row.shares;
        cashNtd += amt;
        updates.push({ range: `H${r}:K${r}`, values: [[1, '', amt, amt]] });
      } else if (row.exchange === 'USD') {
        const amt = row.shares;
        const ntdEquiv = Math.round(amt * usdNtd * 100) / 100;
        cashNtd += ntdEquiv;
        updates.push({ range: `H${r}:K${r}`, values: [[Math.round(usdNtd * 10000) / 10000, '', amt, ntdEquiv]] });
      }
      continue;
    }

    if (row.type === 'STOCK') {
      const isTw = row.exchange === 'TW' || row.exchange === 'TWO';
      const ticker = isTw ? _normalizeTwTicker(row.ticker) : row.ticker;
      const yfTicker = isTw ? `${ticker}.${row.exchange}` : ticker;

      let price = 0;
      let change = 0;
      let changePct = 0;
      let fetchSuccess = false;

      try {
        const res = retryWithBackoff(() => _yfPrice(yfTicker), 3, 1000);
        price = res.price;
        change = res.change;
        changePct = res.changePct;
        fetchSuccess = true;
      } catch (err) {
        logError(fnName, `Failed to fetch price for ${yfTicker}: ${err instanceof Error ? err.message : String(err)}`);
      }

      if (fetchSuccess && price > 0) {
        const absChange = Math.abs(change);
        const absChangePct = Math.abs(changePct);
        const changeEmoji = change >= 0 ? '📈' : '📉';
        const changeSign = change >= 0 ? '+' : '-';

        if (isTw) {
          const twRes = _calculateTwPosition(ticker, row.shares, price, row.totalCost, row.avgCost, holdingLotsMap.get(ticker));
          stockNtd += twRes.netAsset;

          updates.push({
            range: `H${r}:K${r}`,
            values: [[price, Math.round(twRes.netRoi * 1000000) / 1000000, twRes.netAsset, twRes.netAsset]],
          });

          const plSign = twRes.netPL >= 0 ? '+' : '-';
          const plPct = Math.abs(twRes.netRoi * 100);
          twBlocks.push([
            `▸ ${row.name} | ${changeEmoji}${changeSign}${_fmtNum(absChange, 2)} (${absChangePct.toFixed(2)}%)`,
            `   市價 ${_fmtNum(price, 2)} / ${_fmtNum(twRes.netAsset, 0)}`,
            `   成本 ${_fmtNum(twRes.buyPriceAvg, 2)} / ${_fmtNum(row.totalCost, 0)}`,
            `   總損益 ${plSign}${_fmtNum(Math.abs(twRes.netPL), 0)} (${plSign}${plPct.toFixed(2)}%)`,
          ]);
        } else {
          const usRes = _calculateUsPosition(ticker, row.shares, price, row.totalCost, row.avgCost, holdingLotsMap.get(ticker));
          const ntdAsset = Math.round(usRes.netAsset * usdNtd * 100) / 100;
          stockNtd += ntdAsset;

          updates.push({
            range: `H${r}:K${r}`,
            values: [[price, Math.round(usRes.netRoi * 1000000) / 1000000, usRes.netAsset, ntdAsset]],
          });

          const plSign = usRes.netPL >= 0 ? '+' : '-';
          const plPct = Math.abs(usRes.netRoi * 100);
          usBlocks.push([
            `▸ ${row.name} | ${changeEmoji}${changeSign}${_fmtNum(absChange, 2)} (${absChangePct.toFixed(2)}%)`,
            `   市價 ${_fmtNum(price, 2)} / ${_fmtNum(usRes.netAsset, 2)}`,
            `   成本 ${_fmtNum(usRes.buyPriceAvg, 2)} / ${_fmtNum(row.totalCost, 2)}`,
            `   總損益 ${plSign}${_fmtNum(Math.abs(usRes.netPL), 2)} (${plSign}${Math.abs(plPct).toFixed(2)}%)`,
          ]);
        }
      } else {
        const header = change !== 0
          ? `▸ ${row.name} | ${change >= 0 ? '📈+' : '📉-'}${_fmtNum(Math.abs(change), 2)} (${Math.abs(changePct).toFixed(2)}%)`
          : `▸ ${row.name}`;
        if (isTw) {
          twBlocks.push([
            header,
            `   市價 N/A`,
            `   成本 ${_fmtNum(row.avgCost, 2)} / ${_fmtNum(row.totalCost, 0)}`,
            `   總損益 N/A`,
          ]);
        } else {
          usBlocks.push([
            header,
            `   市價 N/A`,
            `   成本 ${_fmtNum(row.avgCost, 2)} / ${_fmtNum(row.totalCost, 2)}`,
            `   總損益 N/A`,
          ]);
        }
      }
    }
  }

  // Write N2 and O2 on loan row
  if (loanRow) {
    const totalAssetNtd = Math.round((stockNtd + cashNtd + loanVal) * 100) / 100;
    const roiTotal = loanVal !== 0 ? Math.round(totalAssetNtd / Math.abs(loanVal) * 1000000) / 1000000 : 0;
    updates.push({ range: `N${loanRow.sheetRow}:O${loanRow.sheetRow}`, values: [[totalAssetNtd, roiTotal]] });
  }

  _batchWrite(updates);
  logInfo(fnName, `Sheet updated: ${updates.length} ranges`);

  // Build LINE report
  const lines: string[] = [
    `📊 投資組合報告 (${label})`,
    `${now} UTC+8`,
    DIVIDER,
  ];

  if (twBlocks.length > 0) {
    lines.push('【台股】', '');
    lines.push(twBlocks.map(b => b.join('\n')).join('\n\n'));
  }
  if (twBlocks.length > 0 && usBlocks.length > 0) lines.push('');
  if (usBlocks.length > 0) {
    lines.push('【美股】', '');
    lines.push(usBlocks.map(b => b.join('\n')).join('\n\n'));
  }
  lines.push(DIVIDER);

  const cashNtdOnly = rows.find(r => r.type === 'CASH' && r.exchange === 'NTD')?.shares ?? 0;
  const cashUsd = rows.find(r => r.type === 'CASH' && r.exchange === 'USD')?.shares ?? 0;
  lines.push(`💰現金: ${_fmtNtd(cashNtd)}`);
  lines.push(`     ${_fmtNtd(cashNtdOnly)} + US$${_fmtNum(cashUsd, 2)}`);

  if (loanRow) {
    const interest = _loanAccruedInterest(loanRow.loanDate ?? '', loanRow.shares, loanRow.annualRate ?? 0);
    const totalAssetNtd = stockNtd + cashNtd + loanVal;
    const totalLoan = loanRow.shares + interest;
    const ror = totalLoan > 0 ? totalAssetNtd / totalLoan * 100 : 0;
    const netSign = totalAssetNtd >= 0 ? '+' : '-';
    const netEmoji = totalAssetNtd >= 0 ? '📈' : '📉';
    lines.push(`💳貸款: ${_fmtNtd(loanRow.shares)} (+${_fmtNum(Math.round(interest), 0)})`);
    lines.push(`${netEmoji}總資產: ${netSign}${_fmtNtd(Math.abs(Math.round(totalAssetNtd)))} (${_pct(ror, false)})`);
  }

  const message = lines.join('\n');
  if (replyToken) {
    sendReplyMessage(replyToken, message);
  } else {
    sendPushMessage(message);
  }
  logInfo(fnName, `Portfolio report sent (${replyToken ? 'reply' : 'push'})`);
}
