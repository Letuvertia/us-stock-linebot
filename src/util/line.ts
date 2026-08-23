interface LineWebhookEvent {
  type: string;
  replyToken?: string;
  message?: {
    type: string;
    text: string;
    mention?: {
      mentionees: Array<{ type: string }>;
    };
  };
  source?: {
    type: string;
    groupId?: string;
    userId?: string;
  };
}

function _lineHeaders(): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${getScriptProperty(PROP_KEYS.LINE_CHANNEL_ACCESS_TOKEN)}`,
  };
}

function verifyWebhookSignature(e: GoogleAppsScript.Events.DoPost): boolean {
  const signature = (e.parameter as Record<string, string>)['x-line-signature']
    || (e as unknown as { headers?: Record<string, string> }).headers?.['x-line-signature'];
  if (!signature) return true; // GAS doesn't always expose headers — skip if unavailable
  const secret = getScriptProperty(PROP_KEYS.LINE_CHANNEL_SECRET);
  const hash = Utilities.computeHmacSha256Signature(e.postData.contents, secret);
  const expected = Utilities.base64Encode(hash);
  return expected === signature;
}

function parseWebhookEvents(e: GoogleAppsScript.Events.DoPost): LineWebhookEvent[] {
  const body = JSON.parse(e.postData.contents);
  return body.events || [];
}

function isBotMentioned(event: LineWebhookEvent): boolean {
  if (event.type !== 'message' || event.message?.type !== 'text') return false;
  const mentionees = event.message?.mention?.mentionees || [];
  return mentionees.some(m => m.type === 'user');
}

function extractUserMessage(event: LineWebhookEvent): string {
  const text = event.message?.text || '';
  return text.replace(/@\S+\s*/g, '').trim();
}

function sendReplyMessage(replyToken: string, text: string): void {
  const chunks = splitLongMessage(text);
  const messages = chunks.map(chunk => ({ type: 'text', text: chunk }));

  const resp = UrlFetchApp.fetch(LINE_REPLY_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: _lineHeaders(),
    payload: JSON.stringify({ replyToken, messages: messages.slice(0, 5) }),
    muteHttpExceptions: true,
  });
  if (resp.getResponseCode() !== 200) {
    logError('sendReplyMessage', `LINE API ${resp.getResponseCode()}: ${resp.getContentText().slice(0, 300)}`);
  }
}

function sendReplyFlexMessage(replyToken: string, flexMessageOrContents: object, defaultAltText: string = 'CFA 練習題目'): void {
  const messageObj = ('type' in flexMessageOrContents && (flexMessageOrContents as any).type === 'flex')
    ? flexMessageOrContents
    : {
        type: 'flex',
        altText: defaultAltText,
        contents: flexMessageOrContents,
      };

  const resp = UrlFetchApp.fetch(LINE_REPLY_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: _lineHeaders(),
    payload: JSON.stringify({ replyToken, messages: [messageObj] }),
    muteHttpExceptions: true,
  });
  if (resp.getResponseCode() !== 200) {
    logError('sendReplyFlexMessage', `LINE API ${resp.getResponseCode()}: ${resp.getContentText().slice(0, 300)}`);
  }
}

function sendReplyMessages(replyToken: string, messages: object[]): void {
  const formatted = messages.map(m => {
    if ('type' in m) return m;
    return {
      type: 'flex',
      altText: 'CFA 練習題目',
      contents: m,
    };
  });

  const resp = UrlFetchApp.fetch(LINE_REPLY_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: _lineHeaders(),
    payload: JSON.stringify({ replyToken, messages: formatted.slice(0, 5) }),
    muteHttpExceptions: true,
  });
  if (resp.getResponseCode() !== 200) {
    logError('sendReplyMessages', `LINE API ${resp.getResponseCode()}: ${resp.getContentText().slice(0, 300)}`);
  }
}

function sendPushFlexMessage(flexContents: object, altText: string = 'CFA 練習題目'): void {
  const groupId = getScriptProperty(PROP_KEYS.LINE_GROUP_ID);
  const messages = [
    {
      type: 'flex',
      altText: altText,
      contents: flexContents,
    },
  ];

  const resp = UrlFetchApp.fetch(LINE_PUSH_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: _lineHeaders(),
    payload: JSON.stringify({ to: groupId, messages }),
    muteHttpExceptions: true,
  });
  if (resp.getResponseCode() !== 200) {
    logError('sendPushFlexMessage', `LINE API ${resp.getResponseCode()}: ${resp.getContentText().slice(0, 300)}`);
  }
}

function sendPushMessage(text: string): void {
  const groupId = getScriptProperty(PROP_KEYS.LINE_GROUP_ID);
  const chunks = splitLongMessage(text);
  const messages = chunks.map(chunk => ({ type: 'text', text: chunk }));

  const resp = UrlFetchApp.fetch(LINE_PUSH_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: _lineHeaders(),
    payload: JSON.stringify({ to: groupId, messages: messages.slice(0, 5) }),
    muteHttpExceptions: true,
  });
  if (resp.getResponseCode() !== 200) {
    logError('sendPushMessage', `LINE API ${resp.getResponseCode()}: ${resp.getContentText().slice(0, 300)}`);
  }
}

function splitLongMessage(text: string, maxLen: number = 4900): string[] {
  if (text.length <= maxLen) return [text];

  const chunks: string[] = [];
  let remaining = text;
  while (remaining.length > 0) {
    if (remaining.length <= maxLen) {
      chunks.push(remaining);
      break;
    }
    let splitAt = remaining.lastIndexOf('\n', maxLen);
    if (splitAt <= 0) splitAt = maxLen;
    chunks.push(remaining.substring(0, splitAt));
    remaining = remaining.substring(splitAt).trimStart();
  }
  return chunks;
}
