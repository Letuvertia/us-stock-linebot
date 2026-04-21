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

      const reactions = [
        '(歪頭)', '(搖尾巴)', '(發呆)', '(打哈欠)', '(聞聞)',
        '(翻肚皮)', '(轉圈圈)', '(趴下)', '(伸懶腰)', '(抓耳朵)',
        '(豎起耳朵)', '(皺鼻子)', '(躲在桌下)', '(搖屁股)',
      ];
      const a = reactions[Math.floor(Math.random() * reactions.length)];
      let b = reactions[Math.floor(Math.random() * reactions.length)];
      while (b === a) b = reactions[Math.floor(Math.random() * reactions.length)];
      const fallback = `${a} ${b}`;
      const reply = withErrorHandling(fnName, () => handleChatQuery(userMessage), fallback);
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
