function _getLogSheet(): GoogleAppsScript.Spreadsheet.Sheet {
  return getOrCreateSheet(SHEET_NAMES.SYSTEM_LOGS, SHEET_HEADERS[SHEET_NAMES.SYSTEM_LOGS]);
}

function _log(level: string, functionName: string, message: string): void {
  try {
    const timestamp = Utilities.formatDate(new Date(), TIMEZONE, 'yyyy-MM-dd HH:mm:ss');
    _getLogSheet().appendRow([timestamp, level, functionName, message]);
  } catch (_) {
    console.log(`[${level}] ${functionName}: ${message}`);
  }
}

function logInfo(functionName: string, message: string): void {
  _log('INFO', functionName, message);
}

function logWarn(functionName: string, message: string): void {
  _log('WARN', functionName, message);
}

function logError(functionName: string, message: string, error?: Error): void {
  const msg = error ? `${message} | ${error.message}` : message;
  _log('ERROR', functionName, msg);
}
