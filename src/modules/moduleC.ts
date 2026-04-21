function handleWebhook(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  const fnName = 'handleWebhook';

  try {
    const events = parseWebhookEvents(e);

    for (const event of events) {
      if (event.source?.groupId) {
        logInfo(fnName, `Group ID: ${event.source.groupId}`);
      }

      if (event.type !== 'message' || event.message?.type !== 'text') continue;
      if (!isBotMentioned(event)) continue;

      const userMessage = extractUserMessage(event);
      if (!userMessage) continue;

      logInfo(fnName, `Received query: ${truncate(userMessage, 50)}`);

      const reply = withErrorHandling(fnName, () => handleChatQuery(userMessage), '抱歉，目前無法處理您的請求，請稍後再試。');
      if (reply && event.replyToken) {
        sendReplyMessage(event.replyToken, reply);
      }
    }
  } catch (e) {
    const error = e instanceof Error ? e : new Error(String(e));
    logError(fnName, 'Webhook processing failed', error);
  }

  return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
}
