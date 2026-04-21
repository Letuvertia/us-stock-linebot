interface NewsItem {
  id: string;
  date: Date;
  tickerTags: string[];
  title: string;
  snippet: string;
  url: string;
  processedAt: Date;
}

function fetchRSSFeed(feedConfig: RSSFeedConfig): NewsItem[] {
  const response = UrlFetchApp.fetch(feedConfig.url, { muteHttpExceptions: true });
  if (response.getResponseCode() !== 200) {
    logWarn('fetchRSSFeed', `RSS fetch failed for ${feedConfig.name}: ${response.getResponseCode()}`);
    return [];
  }

  const xml = XmlService.parse(response.getContentText());
  const root = xml.getRootElement();
  const channel = root.getChild('channel');
  if (!channel) return [];

  const items = channel.getChildren('item');
  const watchlist = getWatchlist();
  const results: NewsItem[] = [];

  for (const item of items) {
    const title = item.getChildText('title') || '';
    const link = item.getChildText('link') || '';
    const pubDate = item.getChildText('pubDate') || '';
    const description = item.getChildText('description') || '';

    const snippet = truncate(stripHtmlTags(description), NEWS_SNIPPET_LENGTH);
    const tags = extractTickerTags(title + ' ' + snippet, watchlist);
    const date = pubDate ? new Date(pubDate) : new Date();

    results.push({
      id: Utilities.getUuid(),
      date,
      tickerTags: tags,
      title: title.trim(),
      snippet,
      url: link.trim(),
      processedAt: new Date(),
    });
  }

  return results;
}

function extractTickerTags(text: string, watchlist: string[]): string[] {
  const upper = text.toUpperCase();
  return watchlist.filter(ticker => {
    const pattern = new RegExp(`\\b${ticker}\\b`, 'i');
    return pattern.test(upper);
  });
}

function storeNewsItems(items: NewsItem[]): void {
  if (items.length === 0) return;

  const existingUrls = new Set(
    getAllRows(SHEET_NAMES.NEWS_STORE).map(row => String(row[5]))
  );

  const newItems = items.filter(item => !existingUrls.has(item.url));
  if (newItems.length === 0) return;

  const rows = newItems.map(item => [
    item.id,
    Utilities.formatDate(item.date, TIMEZONE, 'yyyy-MM-dd HH:mm:ss'),
    item.tickerTags.join(','),
    item.title,
    item.snippet,
    item.url,
    Utilities.formatDate(item.processedAt, TIMEZONE, 'yyyy-MM-dd HH:mm:ss'),
  ]);

  appendRows(SHEET_NAMES.NEWS_STORE, rows);
  logInfo('storeNewsItems', `Stored ${rows.length} new articles`);
}

function getNewsForTicker(ticker: string, days: number): NewsItem[] {
  const cutoff = getDaysAgo(days);
  const rows = getAllRows(SHEET_NAMES.NEWS_STORE);

  return rows
    .filter(row => {
      const tags = String(row[2]).split(',');
      const date = new Date(String(row[1]));
      return tags.includes(ticker) && date >= cutoff;
    })
    .map(row => ({
      id: String(row[0]),
      date: new Date(String(row[1])),
      tickerTags: String(row[2]).split(','),
      title: String(row[3]),
      snippet: String(row[4]),
      url: String(row[5]),
      processedAt: new Date(String(row[6])),
    }))
    .sort((a, b) => b.date.getTime() - a.date.getTime());
}

function deleteOldNews(): number {
  const cutoff = getDaysAgo(NEWS_RETENTION_DAYS);
  return deleteRowsByFilter(SHEET_NAMES.NEWS_STORE, (row) => {
    const date = new Date(String(row[1]));
    return date < cutoff;
  });
}

function collectAllNews(): void {
  let totalStored = 0;

  for (const feed of RSS_FEEDS) {
    const items = withErrorHandling('collectAllNews', () => fetchRSSFeed(feed), []);
    if (items && items.length > 0) {
      storeNewsItems(items);
      totalStored += items.length;
    }
    sleep(500);
  }

  logInfo('collectAllNews', `Collected from ${RSS_FEEDS.length} feeds, ${totalStored} items processed`);
}
