function executeNewsAnalysis(): void {
  const fnName = 'executeNewsAnalysis';
  logInfo(fnName, 'Starting news-driven analysis');

  const watchlist = getWatchlist();
  if (watchlist.length === 0) {
    logWarn(fnName, 'Watchlist is empty, skipping analysis');
    return;
  }

  const date = formatDateTW(new Date());
  let header = `📰 每日新聞分析報告 (${date})\n`;
  header += `監控個股: ${watchlist.join(', ')}\n`;
  header += `═══════════════════\n\n`;
  sendPushMessage(header);

  let analyzed = 0;

  for (const ticker of watchlist) {
    const news = getNewsForTicker(ticker, NEWS_RETENTION_DAYS);
    if (news.length === 0) {
      logInfo(fnName, `No recent news for ${ticker}, skipping`);
      continue;
    }

    logInfo(fnName, `Analyzing ${ticker} with ${news.length} articles`);

    const analysis = withErrorHandling(fnName, () => analyzeStockWithNews(ticker, news));
    if (!analysis) {
      logWarn(fnName, `Analysis failed for ${ticker}`);
      continue;
    }

    const message = `【${ticker} 分析】\n\n${analysis}`;
    sendPushMessage(message);
    analyzed++;

    sleep(GEMINI_DELAY_MS);
  }

  const summary = `\n═══════════════════\n✅ 分析完成：共 ${analyzed}/${watchlist.length} 檔個股`;
  sendPushMessage(summary);
  logInfo(fnName, `Analysis complete: ${analyzed}/${watchlist.length} stocks`);
}
