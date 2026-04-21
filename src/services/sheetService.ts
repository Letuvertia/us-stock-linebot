function _getSpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
  const id = getScriptProperty(PROP_KEYS.SPREADSHEET_ID);
  return SpreadsheetApp.openById(id);
}

function getOrCreateSheet(name: string, headers: string[]): GoogleAppsScript.Spreadsheet.Sheet {
  const ss = _getSpreadsheet();
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(headers);
    sheet.getRange(1, 1, 1, headers.length).setFontWeight('bold');
  }
  return sheet;
}

function appendRows(sheetName: string, rows: unknown[][]): void {
  if (rows.length === 0) return;
  const sheet = getOrCreateSheet(sheetName, SHEET_HEADERS[sheetName]);
  sheet.getRange(sheet.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);
}

function getAllRows(sheetName: string): unknown[][] {
  const sheet = _getSpreadsheet().getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() <= 1) return [];
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
}

function deleteRowsByFilter(sheetName: string, shouldDelete: (row: unknown[]) => boolean): number {
  const sheet = _getSpreadsheet().getSheetByName(sheetName);
  if (!sheet || sheet.getLastRow() <= 1) return 0;
  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
  let deleted = 0;
  for (let i = data.length - 1; i >= 0; i--) {
    if (shouldDelete(data[i])) {
      sheet.deleteRow(i + 2);
      deleted++;
    }
  }
  return deleted;
}

function getConfigValue(key: string): string | null {
  const rows = getAllRows(SHEET_NAMES.USER_CONFIG);
  for (const row of rows) {
    if (row[0] === key) return String(row[1]);
  }
  return null;
}

function setConfigValue(key: string, value: string): void {
  const sheet = getOrCreateSheet(SHEET_NAMES.USER_CONFIG, SHEET_HEADERS[SHEET_NAMES.USER_CONFIG]);
  const data = getAllRows(SHEET_NAMES.USER_CONFIG);
  for (let i = 0; i < data.length; i++) {
    if (data[i][0] === key) {
      sheet.getRange(i + 2, 2).setValue(value);
      return;
    }
  }
  sheet.appendRow([key, value]);
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
