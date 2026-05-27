// GAS-reserved entry points — `doGet` and `doPost` are the function names GAS
// calls when the deployment's `/exec` URL is hit. They must keep these exact
// names. Real dispatching logic lives in the handle* functions below.

function doPost(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  return handleLocalWebhook(e);
}

function doGet(e: GoogleAppsScript.Events.DoGet): GoogleAppsScript.Content.TextOutput {
  // Token-guarded admin endpoint — used by CI to reconcile triggers.
  // Runs as the deployment owner, so it has the script.scriptapp scope that
  // clasp run cannot grant (clasp's hardcoded scope list omits it).
  if (e.parameter.action === 'install-triggers') {
    const expected = getScriptProperty(PROP_KEYS.INSTALL_TRIGGERS_TOKEN);
    if (!expected || e.parameter.token !== expected) {
      return ContentService.createTextOutput('Forbidden').setMimeType(ContentService.MimeType.TEXT);
    }
    installTriggers();
    return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
  }
  return ContentService.createTextOutput('Not found').setMimeType(ContentService.MimeType.TEXT);
}

function handleLocalWebhook(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  try {
    const body = JSON.parse(e.postData.contents);
    if (body.event === 'summaries_updated') {
      withErrorHandling('handleSummariesUpdated', () => handleSummariesUpdated(body.ids || []));
      return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
    }
    if (body.event === 'podcast_summarized') {
      withErrorHandling('handlePodcastSummarized', () => handlePodcastSummarized(body));
      return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
    }
  } catch (_) {
    // not JSON or missing event field — fall through to LINE chat webhook handler
  }
  return handleChatWebhook(e);
}
