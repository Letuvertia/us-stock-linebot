// `doPost` is the GAS-reserved entry point — when the deployment's `/exec`
// URL receives a POST, GAS calls this function. It must keep this exact name.
// Real dispatching logic lives in handleLocalWebhook below.

function doPost(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  return handleLocalWebhook(e);
}

function handleLocalWebhook(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  try {
    const body = JSON.parse(e.postData.contents);
    if (body.event === 'summaries_updated') {
      withErrorHandling('handleSummariesUpdated', () => handleSummariesUpdated(body.ids || []));
      return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
    }
  } catch (_) {
    // not JSON or missing event field — fall through to LINE chat webhook handler
  }
  return handleChatWebhook(e);
}
