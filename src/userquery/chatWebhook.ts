function _isTriggered(event: LineWebhookEvent): boolean {
  if (isBotMentioned(event)) return true;
  const text = event.message?.text || '';
  return text.trimStart().startsWith('皮皮');
}

function _stripTrigger(text: string): string {
  // Strip @mentions then leading 皮皮
  const noMention = text.replace(/@\S+\s*/g, '').trim();
  return noMention.startsWith('皮皮') ? noMention.slice(2).trim() : noMention;
}

function _dispatch(text: string, replyToken: string): void {
  if (text.includes('目標價')) {
    const result = queryTargetPriceByCategory(text);
    if (result !== null) {
      sendReplyMessage(replyToken, result);
      return;
    }
  }

  // Fallback: random reaction
  const reactions = [
    '(歪頭)', '(搖尾巴)', '(發呆)', '(打哈欠)', '(聞聞)',
    '(翻肚皮)', '(轉圈圈)', '(趴下)', '(伸懶腰)', '(抓耳朵)',
    '(豎起耳朵)', '(皺鼻子)', '(躲在桌下)', '(搖屁股)',
  ];
  let a = reactions[Math.floor(Math.random() * reactions.length)];
  let b = reactions[Math.floor(Math.random() * reactions.length)];
  while (b === a) b = reactions[Math.floor(Math.random() * reactions.length)];
  sendReplyMessage(replyToken, `${a} ${b}`);
}

function handleWebhook(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  const fnName = 'handleWebhook';

  try {
    const events = parseWebhookEvents(e);

    for (const event of events) {
      if (event.source?.groupId) {
        logInfo(fnName, `Group ID: ${event.source.groupId}`);
      }

      if (event.type !== 'message' || event.message?.type !== 'text') continue;
      if (!_isTriggered(event)) continue;

      const userMessage = _stripTrigger(event.message?.text || '');
      if (!userMessage) continue;

      logInfo(fnName, `Received query: ${truncate(userMessage, 50)}`);

      if (event.replyToken) {
        _dispatch(userMessage, event.replyToken);
      }
    }
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    logError(fnName, 'Webhook processing failed', error);
  }

  return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
}
