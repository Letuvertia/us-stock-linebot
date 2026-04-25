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
