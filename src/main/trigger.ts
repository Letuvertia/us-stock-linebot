// ===== Trigger handlers =====
// Each function here is the entry point for a scheduled trigger.
// To add a new schedule, add a handler + an entry to _triggerSpecs() below.

function runPortfolioTWClose(): void {
  withErrorHandling('runPortfolioTWClose', () => {
    executePortfolioReport('台股收盤');
  });
}

function runPortfolioUSClose(): void {
  withErrorHandling('runPortfolioUSClose', () => {
    executePortfolioReport('美股收盤');
  });
}

// ===== Declarative trigger spec =====
// installTriggers() reconciles the project's installed triggers to match this list.
// GAS doesn't expose schedule info from existing triggers, so we wipe-and-reinstall —
// idempotent and the cheapest correct approach. Called by the deploy workflow after clasp push.

interface TriggerSpec {
  handler: string;
  hour: number;
  minute: number;
  weekDays: GoogleAppsScript.Base.Weekday[];
}

function _triggerSpecs(): TriggerSpec[] {
  return [
    {
      handler: 'runPortfolioTWClose',
      hour: 14,
      minute: 0,
      weekDays: [
        ScriptApp.WeekDay.MONDAY,
        ScriptApp.WeekDay.TUESDAY,
        ScriptApp.WeekDay.WEDNESDAY,
        ScriptApp.WeekDay.THURSDAY,
        ScriptApp.WeekDay.FRIDAY,
      ],
    },
    {
      handler: 'runPortfolioUSClose',
      hour: 4,
      minute: 30,
      weekDays: [
        ScriptApp.WeekDay.TUESDAY,
        ScriptApp.WeekDay.WEDNESDAY,
        ScriptApp.WeekDay.THURSDAY,
        ScriptApp.WeekDay.FRIDAY,
        ScriptApp.WeekDay.SATURDAY,
      ],
    },
  ];
}

function installTriggers(): void {
  withErrorHandling('installTriggers', () => {
    const existing = ScriptApp.getProjectTriggers();
    existing.forEach(t => ScriptApp.deleteTrigger(t));
    logInfo('installTriggers', `Removed ${existing.length} existing triggers`);

    let added = 0;
    for (const spec of _triggerSpecs()) {
      for (const day of spec.weekDays) {
        ScriptApp.newTrigger(spec.handler)
          .timeBased()
          .atHour(spec.hour)
          .nearMinute(spec.minute)
          .onWeekDay(day)
          .inTimezone(TIMEZONE)
          .create();
        added++;
      }
      logInfo(
        'installTriggers',
        `Installed ${spec.handler} at ${spec.hour}:${String(spec.minute).padStart(2, '0')} on ${spec.weekDays.length} days`,
      );
    }

    logInfo('installTriggers', `Done — removed=${existing.length}, added=${added}`);
  });
}
