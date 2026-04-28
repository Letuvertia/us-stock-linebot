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

// Column indices in StockUniverse sheet (0-based, matches 173-col schema)
const COL = {
  TICKER: 0, EXCHANGE: 1, NAME: 2,
  PRICE: 12,       // Current_Price
  CHANGE: 13,      // Change_Pct
  PREV_CLOSE: 14,  // Prev_Close
  W52_HIGH: 15,    // 52W_High
  W52_LOW: 17,     // 52W_Low
  DIST_HIGH: 19,   // Dist_From_High_Pct
  BETA: 33,        // Beta
  PE: 41,          // PE_TTM
  EPS: 50,         // EPS_TTM
  EPS_GROWTH: 53,  // EPS_Growth_TTM_YoY
  DIV_YIELD: 102,  // Dividend_Yield
  OP_MARGIN: 72,   // Operating_Margin
  NET_MARGIN: 74,  // Net_Margin
  ROE: 79,         // ROE
  MCAP: 109,       // Market_Cap_M
  EPS_LQ_ACT: 122,      // EPS_LQ_Act
  EPS_LQ_SURPRISE: 123, // EPS_LQ_Surprise
  INDUSTRY: 124,   // Industry
  PEERS: 127,      // Peers (comma-separated)
  UPDATED: 137,    // MW_Updated_At
  FWD_PE: 139,     // Finnhub_Forward_PE
  PEG: 141,        // Finnhub_PEG
  STRONG_BUY: 146, // Finnhub_StrongBuy
  BUY: 147,        // Finnhub_Buy
  HOLD: 148,       // Finnhub_Hold
  SELL: 149,       // Finnhub_Sell
  STRONG_SELL: 150,// Finnhub_StrongSell
  RATING: 151,     // Finnhub_Rating_Score
  MW_NUM_RATINGS: 154,    // MW_Num_Ratings
  TARGET_HIGH: 159,       // FMP_Target_High
  TARGET_LOW: 160,        // FMP_Target_Low
  TARGET_CONSENSUS: 161,  // FMP_Target_Consensus
  TARGET_MEDIAN: 162,     // FMP_Target_Median
  UPSIDE: 163,            // FMP_Upside_Pct
  MW_TARGET_HIGH: 164,    // MW_Target_High
  MW_TARGET_LOW: 165,     // MW_Target_Low
  MW_TARGET_MEDIAN: 166,  // MW_Target_Median
  MW_TARGET_AVG: 167,     // MW_Target_Avg
  MW_UPSIDE: 168,         // MW_Upside_Pct
  MW_EPS_FY1: 169,        // MW_EPS_FY1_Avg
  MW_EPS_FY2: 170,        // MW_EPS_FY2_Avg
  MW_EPS_LQ_EST: 171,     // MW_EPS_LQ_Est
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
      mwEpsLQAct: _num(row, COL.EPS_LQ_ACT),
      mwEpsLQSurprise: _num(row, COL.EPS_LQ_SURPRISE),
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

const ROADS = [
  '忠孝東路', '敦化南路', '中山北路', '仁愛路', '信義路',
  '和平東路', '羅斯福路', '南京東路', '民權西路', '重慶南路',
  '基隆路', '復興南路', '承德路', '林森北路', '光復南路',
  '建國北路', '辛亥路', '迪化街', '青田街', '凱達格蘭大道',
];

const LOCATIONS = [
  '電線桿旁', '行道樹下', '樹叢裡', '7-11 前面', '變電箱後方',
  '公車亭座椅下', 'YouBike 站柱旁', '捷運站出口', '消防栓邊',
  '騎樓柱子後', '垃圾桶旁', '警衛室門口', '自動販賣機下',
  '公用電話亭內', '停車場繳費機旁', '天橋樓梯口', '巷口紅磚牆邊',
  '公園長椅下', '排水溝蓋上', '告示牌背面',
];

function _pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function formatStockRanking(stocks: StockCandidate[]): string {
  const date = formatDateTW(new Date());
  const road = _pick(ROADS);
  const location = _pick(LOCATIONS);
  let msg = `皮皮在${road}的${location}找到了一份資料！\n`;
  msg += `─────────────────\n\n`;
  msg += `📊 美股低估排行 (${date})\n`;
  msg += `基準: 分析師目標價 + 評等\n\n`;

  stocks.forEach((s, i) => {
    const rating = _ratingLabel(s.ratingScore);
    msg += `${i + 1}. ${s.ticker} (${s.name})\n`;
    const arrow = s.changePct >= 0 ? '📈' : '📉';
    msg += `   ${arrow} $${s.currentPrice} (${s.changePct >= 0 ? '+' : ''}${s.changePct}%)\n`;
    const tAvg = s.mwTargetAvg ?? s.targetConsensus;
    const tLow = s.mwTargetLow ?? s.targetLow;
    const tHigh = s.mwTargetHigh ?? s.targetHigh;
    const upside = _bestUpside(s);
    msg += `   目標均價: $${tAvg} ($${tLow}~$${tHigh})\n`;
    msg += `   潛在漲幅: +${upside}%\n`;

    msg += `   評等: ${rating} (${s.ratingScore}/5)\n`;
    msg += `   [${s.strongBuy}/${s.buy}/${s.hold}/${s.sell}/${s.strongSell}]`;
    if (s.mwNumRatings) msg += ` ${s.mwNumRatings}人`;
    msg += `\n`;

    const extras: string[] = [];
    if (s.forwardPE) extras.push(`FwdPE:${s.forwardPE}`);
    if (s.beta) extras.push(`β:${s.beta}`);
    if (extras.length > 0) msg += `   ${extras.join(' ')}\n`;

    const eps: string[] = [];
    if (s.mwEpsFY1) eps.push(`EPS:${s.mwEpsFY1}`);
    if (s.mwEpsFY2) eps.push(`→${s.mwEpsFY2}`);
    if (s.mwEpsLQSurprise !== null) eps.push(`驚喜:${s.mwEpsLQSurprise > 0 ? '+' : ''}${s.mwEpsLQSurprise}`);
    if (eps.length > 0) msg += `   ${eps.join(' ')}\n`;

    if (i < stocks.length - 1) msg += `\n`;
  });

  msg += `─────────────────\n`;
  msg += `資料更新: ${stocks[0]?.updatedAt || 'N/A'}\n`;
  msg += `共 ${stocks.length} 檔符合條件 (評等≥中立 且 目標價有上漲空間)`;
  return msg;
}

function executeStockScan(label: string): void {
  const fnName = 'executeStockScan';
  logInfo(fnName, `Starting ${label} scan`);

  const candidates = loadStocksFromSheet();
  if (candidates.length === 0) {
    logWarn(fnName, 'No stock data in StockUniverse (run data collector first)');
    return;
  }

  logInfo(fnName, `Loaded ${candidates.length} stocks from sheet`);

  const ranked = rankStocks(candidates);
  if (ranked.length === 0) {
    logWarn(fnName, 'No stocks passed filtering criteria');
    return;
  }

  const message = formatStockRanking(ranked);
  sendPushMessage(message);
  logInfo(fnName, `Pushed top ${ranked.length} stocks to LINE`);
}
