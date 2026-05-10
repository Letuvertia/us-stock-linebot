function getNowTaiwan(): Date {
  return new Date(Utilities.formatDate(new Date(), TIMEZONE, "yyyy-MM-dd'T'HH:mm:ss"));
}

function formatDateTW(date: Date): string {
  return Utilities.formatDate(date, TIMEZONE, 'yyyy/MM/dd');
}

function getDaysAgo(days: number): Date {
  const d = new Date();
  d.setDate(d.getDate() - days);
  return d;
}

function isWithinDays(date: Date, days: number): boolean {
  return date.getTime() >= getDaysAgo(days).getTime();
}

function sleep(ms: number): void {
  Utilities.sleep(ms);
}

function getScriptProperty(key: string): string {
  const value = PropertiesService.getScriptProperties().getProperty(key);
  if (!value) throw new Error(`Missing script property: ${key}`);
  return value;
}

function retryWithBackoff<T>(fn: () => T, maxRetries: number = 3, baseDelay: number = 1000): T {
  let lastError: Error | undefined;
  for (let i = 0; i < maxRetries; i++) {
    try {
      return fn();
    } catch (e) {
      lastError = e instanceof Error ? e : new Error(String(e));
      if (i < maxRetries - 1) {
        Utilities.sleep(baseDelay * Math.pow(2, i));
      }
    }
  }
  throw lastError;
}

function withErrorHandling<T>(functionName: string, fn: () => T, fallback?: T): T | undefined {
  try {
    return fn();
  } catch (e) {
    const error = e instanceof Error ? e : new Error(String(e));
    logError(functionName, 'Unhandled error', error);
    return fallback;
  }
}

function stripHtmlTags(html: string): string {
  return html.replace(/<[^>]*>/g, '').trim();
}

function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
}
