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

function runNewsAnalysis(): void {
  withErrorHandling('runNewsAnalysis', () => {
    executeNewsAnalysis();
  });
}

// ===== Setup Functions (run manually in GAS editor) =====

function installTriggers(): void {
  withErrorHandling('installTriggers', () => {
    installAllTriggers();
  });
}

function initializeAllSheets(): void {
  withErrorHandling('initializeAllSheets', () => {
    initializeSheets();
  });
}
