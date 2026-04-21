interface NewsItem {
  id: string;
  date: Date;
  tickerTags: string[];
  title: string;
  snippet: string;
  url: string;
  processedAt: Date;
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
