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
  try {
    const resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() !== 200) return { price: 0, change: 0, changePct: 0 };
    const data = JSON.parse(resp.getContentText());
    const result = data?.chart?.result?.[0];
    if (!result) return { price: 0, change: 0, changePct: 0 };
    const meta = result.meta;
    const price: number = meta.regularMarketPrice ?? 0;
    // Prefer the actual previous candle close over meta field
    const closes: number[] = (result.indicators?.quote?.[0]?.close ?? []).filter((c: number | null) => c != null);
    const prev: number = closes.length >= 2 ? closes[closes.length - 2] : (meta.chartPreviousClose ?? 0);
    const change = Math.round((price - prev) * 100) / 100;
    const changePct = prev > 0 ? (price - prev) / prev * 100 : 0;
    return { price, change, changePct };
  } catch {
    return { price: 0, change: 0, changePct: 0 };
  }
}

function _fetchUsdNtd(): number {
  const { price } = _yfPrice('USDTWD=X');
  return price > 0 ? price : 32.0;
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

function executePortfolioReport(label?: string): void {
  const fnName = 'executePortfolioReport';

  if (!label) {
    const hour = parseInt(Utilities.formatDate(new Date(), TIMEZONE, 'HH'));
    label = (hour >= 12 && hour < 20) ? '台股收盤' : '美股收盤';
  }

  const rows = _loadHoldings();
  const usdNtd = _fetchUsdNtd();
  const now = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy-MM-dd HH:mm');

  const updates: { range: string; values: (string | number)[][] }[] = [];
  const DIVIDER = '──────────────';

  let stockNtd = 0;
  let cashNtd = 0;
  let loanRow: HoldingRow | null = null;
  let loanVal = 0;

  const twLines: string[] = [];
  const usLines: string[] = [];

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
      const yfTicker = isTw ? `${row.ticker}.${row.exchange}` : row.ticker;
      const { price, change, changePct } = retryWithBackoff(() => _yfPrice(yfTicker), 2, 1500);

      const localAsset = Math.round(row.shares * price * 100) / 100;
      const ntdAsset = isTw ? localAsset : Math.round(localAsset * usdNtd * 100) / 100;
      const roi = row.avgCost > 0 ? Math.round((price / row.avgCost - 1) * 1000000) / 1000000 : 0;
      stockNtd += ntdAsset;
      updates.push({ range: `H${r}:K${r}`, values: [[price, roi, localAsset, ntdAsset]] });

      const absChange = Math.abs(change);
      const absChangePct = Math.abs(changePct);
      const changeEmoji = change >= 0 ? '📈' : '📉';
      const changeSign = change >= 0 ? '+' : '-';

      if (isTw) {
        const pl = Math.round(localAsset - row.totalCost);
        const plPct = row.totalCost > 0 ? pl / row.totalCost * 100 : 0;
        const plSign = pl >= 0 ? '+' : '-';
        twLines.push(
          `▸ ${row.name} | ${changeEmoji}${changeSign}${_fmtNum(absChange, 2)} (${absChangePct.toFixed(2)}%)`,
          `   市價 ${_fmtNum(price, 2)} / ${_fmtNum(localAsset, 0)}`,
          `   成本 ${_fmtNum(row.avgCost, 2)} / ${_fmtNum(row.totalCost, 0)}`,
          `   總損益 ${plSign}${_fmtNum(Math.abs(pl), 0)} (${Math.abs(plPct).toFixed(2)}%)`,
        );
      } else {
        const pl = Math.round((localAsset - row.totalCost) * 100) / 100;
        const plPct = row.totalCost > 0 ? pl / row.totalCost * 100 : 0;
        const plSign = pl >= 0 ? '+' : '-';
        usLines.push(
          `▸ ${row.name} | ${changeEmoji}${changeSign}${_fmtNum(absChange, 2)} (${absChangePct.toFixed(2)}%)`,
          `   市價 ${_fmtNum(price, 2)} / ${_fmtNum(localAsset, 2)}`,
          `   成本 ${_fmtNum(row.avgCost, 2)} / ${_fmtNum(row.totalCost, 2)}`,
          `   總損益 ${plSign}${_fmtNum(Math.abs(pl), 2)} (${Math.abs(plPct).toFixed(2)}%)`,
        );
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

  const _interleave = (stockLines: string[]) => {
    const out: string[] = [];
    for (let i = 0; i < stockLines.length; i += 4) {
      if (i > 0) out.push('');
      out.push(...stockLines.slice(i, i + 4));
    }
    return out;
  };

  if (twLines.length > 0) {
    lines.push('【台股】', '');
    lines.push(..._interleave(twLines));
  }
  if (twLines.length > 0 && usLines.length > 0) lines.push('');
  if (usLines.length > 0) {
    lines.push('【美股】', '');
    lines.push(..._interleave(usLines));
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

  sendPushMessage(lines.join('\n'));
  logInfo(fnName, 'Portfolio report sent');
}
