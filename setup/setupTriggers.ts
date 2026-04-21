function removeAllTriggers(): void {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(t => ScriptApp.deleteTrigger(t));
  logInfo('removeAllTriggers', `Removed ${triggers.length} existing triggers`);
}

function installAllTriggers(): void {
  removeAllTriggers();

  // Module A: Pre-market scan (20:30 Taiwan time = US market opens ~21:30)
  ScriptApp.newTrigger('runPreMarketScan')
    .timeBased()
    .atHour(20)
    .nearMinute(30)
    .everyDays(1)
    .inTimezone(TIMEZONE)
    .create();

  // Module A: Post-market scan (05:30 Taiwan time = US market closed ~05:00)
  ScriptApp.newTrigger('runPostMarketScan')
    .timeBased()
    .atHour(5)
    .nearMinute(30)
    .everyDays(1)
    .inTimezone(TIMEZONE)
    .create();

  // Module B: News analysis (daily 18:00 Taiwan time)
  ScriptApp.newTrigger('runNewsAnalysis')
    .timeBased()
    .atHour(18)
    .nearMinute(0)
    .everyDays(1)
    .inTimezone(TIMEZONE)
    .create();

  logInfo('installAllTriggers', 'All 3 triggers installed successfully');
}
