function _getSpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
  const id = getScriptProperty(PROP_KEYS.SPREADSHEET_ID);
  return SpreadsheetApp.openById(id);
}

function getAllRows(sheetName: string): unknown[][] {
  const sheet = _getSpreadsheet().getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() <= 1) return [];
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
}

function getConfigValue(key: string): string | null {
  const rows = getAllRows(SHEET_NAMES.USER_CONFIG);
  for (const row of rows) {
    if (row[0] === key) return String(row[1]);
  }
  return null;
}

function getWatchlist(): string[] {
  const raw = getConfigValue('WATCHLIST');
  if (!raw) return [];
  return JSON.parse(raw) as string[];
}

function getAllTickers(): string[] {
  const rows = getAllRows(SHEET_NAMES.STOCK_UNIVERSE);
  return rows.map(row => String(row[0]));
}
