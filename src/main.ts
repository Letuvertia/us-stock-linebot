// ===== Webhook Entry Point =====

function doPost(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  return handleWebhook(e);
}

// ===== Scheduled Trigger Handlers =====

function runPreMarketScan(): void {
  withErrorHandling('runPreMarketScan', () => {
    executeStockScan('盤前 (Pre-Market)');
  });
}

function runPostMarketScan(): void {
  withErrorHandling('runPostMarketScan', () => {
    executeStockScan('盤後 (Post-Market)');
  });
}

function runIndustryPeReport(): void {
  withErrorHandling('runIndustryPeReport', () => {
    executeIndustryPeReport();
  });
}

// ===== Trigger Setup (run manually in GAS editor) =====

function installTriggers(): void {
  withErrorHandling('installTriggers', () => {
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(t => ScriptApp.deleteTrigger(t));
    logInfo('installTriggers', `Removed ${triggers.length} existing triggers`);

    ScriptApp.newTrigger('runPreMarketScan')
      .timeBased()
      .atHour(20)
      .nearMinute(30)
      .everyDays(1)
      .inTimezone(TIMEZONE)
      .create();

    ScriptApp.newTrigger('runPostMarketScan')
      .timeBased()
      .atHour(5)
      .nearMinute(30)
      .everyDays(1)
      .inTimezone(TIMEZONE)
      .create();

    logInfo('installTriggers', 'Triggers installed: runPreMarketScan (20:30), runPostMarketScan (05:30)');
  });
}
