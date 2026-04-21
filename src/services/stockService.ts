interface StockCandidate {
  ticker: string;
  name: string;
  currentPrice: number;
  changePct: number;
  fiftyTwoWeekHigh: number;
  distFromHigh: number;
  strongBuy: number;
  buy: number;
  hold: number;
  sell: number;
  strongSell: number;
  ratingScore: number;
  peTTM: number | null;
  forwardPE: number | null;
  peg: number | null;
  epsTTM: number | null;
  epsGrowth: number | null;
  beta: number | null;
  marketCapM: number | null;
  divYield: number | null;
  opMargin: number | null;
  netMargin: number | null;
  roe: number | null;
  targetHigh: number | null;
  targetLow: number | null;
  targetConsensus: number | null;
  targetMedian: number | null;
  upsidePct: number | null;
  mwTargetHigh: number | null;
  mwTargetLow: number | null;
  mwTargetMedian: number | null;
  mwTargetAvg: number | null;
  mwNumRatings: number | null;
  mwUpsidePct: number | null;
  mwEpsFY1: number | null;
  mwEpsFY2: number | null;
  mwEpsLQEst: number | null;
  mwEpsLQAct: number | null;
  mwEpsLQSurprise: number | null;
  updatedAt: string;
}

// Column indices in StockUniverse sheet (0-based)
const COL = {
  TICKER: 0, EXCHANGE: 1, NAME: 2,
  PRICE: 3, CHANGE: 4, OPEN: 5, HIGH: 6, LOW: 7, PREV_CLOSE: 8,
  W52_HIGH: 9, W52_LOW: 10, DIST_HIGH: 11,
  STRONG_BUY: 12, BUY: 13, HOLD: 14, SELL: 15, STRONG_SELL: 16, RATING: 17,
  PE: 18, FWD_PE: 19, PEG: 20, EPS: 21, EPS_GROWTH: 22,
  BETA: 23, MCAP: 24, DIV_YIELD: 25,
  OP_MARGIN: 26, NET_MARGIN: 27, ROE: 28,
  INDUSTRY: 29, UPDATED: 30,
  TARGET_HIGH: 31, TARGET_LOW: 32, TARGET_CONSENSUS: 33, TARGET_MEDIAN: 34, UPSIDE: 35,
  FMP_UPDATED: 36,
  MW_TARGET_HIGH: 37, MW_TARGET_LOW: 38, MW_TARGET_MEDIAN: 39, MW_TARGET_AVG: 40,
  MW_NUM_RATINGS: 41, MW_UPSIDE: 42,
  MW_EPS_FY1: 43, MW_EPS_FY2: 44,
  MW_EPS_LQ_EST: 45, MW_EPS_LQ_ACT: 46, MW_EPS_LQ_SURPRISE: 47,
  MW_UPDATED: 48,
} as const;

function _num(row: unknown[], idx: number): number | null {
  const v = row[idx];
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  return isNaN(n) ? null : n;
}

function loadStocksFromSheet(): StockCandidate[] {
  const rows = getAllRows(SHEET_NAMES.STOCK_UNIVERSE);
  const results: StockCandidate[] = [];

  for (const row of rows) {
    const price = _num(row, COL.PRICE);
    if (!price) continue;

    results.push({
      ticker: String(row[COL.TICKER] || ''),
      name: String(row[COL.NAME] || ''),
      currentPrice: price,
      changePct: _num(row, COL.CHANGE) || 0,
      fiftyTwoWeekHigh: _num(row, COL.W52_HIGH) || 0,
      distFromHigh: _num(row, COL.DIST_HIGH) || 0,
      strongBuy: _num(row, COL.STRONG_BUY) || 0,
      buy: _num(row, COL.BUY) || 0,
      hold: _num(row, COL.HOLD) || 0,
      sell: _num(row, COL.SELL) || 0,
      strongSell: _num(row, COL.STRONG_SELL) || 0,
      ratingScore: _num(row, COL.RATING) || 0,
      peTTM: _num(row, COL.PE),
      forwardPE: _num(row, COL.FWD_PE),
      peg: _num(row, COL.PEG),
      epsTTM: _num(row, COL.EPS),
      epsGrowth: _num(row, COL.EPS_GROWTH),
      beta: _num(row, COL.BETA),
      marketCapM: _num(row, COL.MCAP),
      divYield: _num(row, COL.DIV_YIELD),
      opMargin: _num(row, COL.OP_MARGIN),
      netMargin: _num(row, COL.NET_MARGIN),
      roe: _num(row, COL.ROE),
      targetHigh: _num(row, COL.TARGET_HIGH),
      targetLow: _num(row, COL.TARGET_LOW),
      targetConsensus: _num(row, COL.TARGET_CONSENSUS),
      targetMedian: _num(row, COL.TARGET_MEDIAN),
      upsidePct: _num(row, COL.UPSIDE),
      mwTargetHigh: _num(row, COL.MW_TARGET_HIGH),
      mwTargetLow: _num(row, COL.MW_TARGET_LOW),
      mwTargetMedian: _num(row, COL.MW_TARGET_MEDIAN),
      mwTargetAvg: _num(row, COL.MW_TARGET_AVG),
      mwNumRatings: _num(row, COL.MW_NUM_RATINGS),
      mwUpsidePct: _num(row, COL.MW_UPSIDE),
      mwEpsFY1: _num(row, COL.MW_EPS_FY1),
      mwEpsFY2: _num(row, COL.MW_EPS_FY2),
      mwEpsLQEst: _num(row, COL.MW_EPS_LQ_EST),
      mwEpsLQAct: _num(row, COL.MW_EPS_LQ_ACT),
      mwEpsLQSurprise: _num(row, COL.MW_EPS_LQ_SURPRISE),
      updatedAt: String(row[COL.UPDATED] || ''),
    });
  }

  return results;
}

function _bestUpside(s: StockCandidate): number | null {
  if (s.mwUpsidePct !== null) return s.mwUpsidePct;
  return s.upsidePct;
}

function _bestTarget(s: StockCandidate): number | null {
  if (s.mwTargetAvg !== null) return s.mwTargetAvg;
  return s.targetConsensus;
}

function rankStocks(candidates: StockCandidate[]): StockCandidate[] {
  const withTargets = candidates.filter(s => {
    const upside = _bestUpside(s);
    const target = _bestTarget(s);
    return target !== null && upside !== null && upside > 0 && s.ratingScore >= 3.0;
  });

  return withTargets
    .sort((a, b) => {
      const aUp = _bestUpside(a) ?? 0;
      const bUp = _bestUpside(b) ?? 0;
      return bUp - aUp;
    })
    .slice(0, TOP_STOCKS_COUNT);
}

function _ratingLabel(score: number): string {
  if (score >= 4.5) return '強力買進';
  if (score >= 3.8) return '買進';
  if (score >= 3.0) return '中立';
  if (score >= 2.0) return '賣出';
  if (score > 0) return '強力賣出';
  return '-';
}

function formatStockRanking(stocks: StockCandidate[]): string {
  const date = formatDateTW(new Date());
  let msg = `📊 美股低估排行 (${date})\n`;
  msg += `基準: 分析師目標價 + 評等\n`;
  msg += `─────────────────\n\n`;

  stocks.forEach((s, i) => {
    const rating = _ratingLabel(s.ratingScore);
    msg += `${i + 1}. ${s.ticker} (${s.name})\n`;
    msg += `   現價: $${s.currentPrice} (${s.changePct >= 0 ? '+' : ''}${s.changePct}%)\n`;
    const tAvg = s.mwTargetAvg ?? s.targetConsensus;
    const tLow = s.mwTargetLow ?? s.targetLow;
    const tHigh = s.mwTargetHigh ?? s.targetHigh;
    const upside = _bestUpside(s);
    msg += `   目標均價: $${tAvg} ($${tLow}~$${tHigh})\n`;
    msg += `   潛在漲幅: +${upside}%\n`;

    msg += `   評等: ${rating} (${s.ratingScore}/5)`;
    msg += ` [強買${s.strongBuy}/買${s.buy}/持${s.hold}/賣${s.sell}/強賣${s.strongSell}]\n`;

    const extras: string[] = [];
    if (s.peTTM) extras.push(`P/E: ${s.peTTM}`);
    if (s.forwardPE) extras.push(`Fwd P/E: ${s.forwardPE}`);
    if (s.beta) extras.push(`Beta: ${s.beta}`);
    if (s.mwNumRatings) extras.push(`分析師: ${s.mwNumRatings}`);
    if (extras.length > 0) msg += `   ${extras.join('  ')}\n`;

    const eps: string[] = [];
    if (s.mwEpsFY1) eps.push(`EPS FY1: ${s.mwEpsFY1}`);
    if (s.mwEpsFY2) eps.push(`FY2: ${s.mwEpsFY2}`);
    if (s.mwEpsLQSurprise !== null) eps.push(`上季驚喜: ${s.mwEpsLQSurprise > 0 ? '+' : ''}${s.mwEpsLQSurprise}`);
    if (eps.length > 0) msg += `   ${eps.join('  ')}\n`;

    if (i < stocks.length - 1) msg += `\n`;
  });

  msg += `─────────────────\n`;
  msg += `資料更新: ${stocks[0]?.updatedAt || 'N/A'}\n`;
  msg += `共 ${stocks.length} 檔符合條件 (評等≥中立 且 目標價有上漲空間)`;
  return msg;
}
