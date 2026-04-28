const HELP_HEADERS = [
  '皮皮咬著一張指令表',
  '指令表上面有皮皮的口水',
  '皮皮把指令表丟在地上之後開心的看著你！',
  '皮皮把指令表丟在玄關之後跑去廚房了',
  '皮皮想要把指令表藏在沙發後面',
  '皮皮把指令表壓住了',
  '皮皮死咬著指令表不給你',
  '皮皮跑去追蝴蝶了',
  '皮皮盯著牆壁上的螞蟻',
  '皮皮在啃你的拖鞋',
];

const HELP_BODY =
  `──────────────\n` +
  `指標：目標價、P/E\n` +
  `產業分類：公用事業/原材料/工業/房地產/核心消費/能源/資訊科技/通訊服務/醫療保健/金融/非核心消費\n` +
  `個股：直接問股票代碼、英文或中文`;

function _replyWithHelp(replyToken: string, mainText: string): void {
  const header = HELP_HEADERS[Math.floor(Math.random() * HELP_HEADERS.length)];
  const helpChunk = { type: 'text', text: `${header}\n${HELP_BODY}` };
  const mainChunks = splitLongMessage(mainText).map(t => ({ type: 'text', text: t }));
  const messages = [...mainChunks, helpChunk].slice(0, 5);

  UrlFetchApp.fetch(LINE_REPLY_URL, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${getScriptProperty(PROP_KEYS.LINE_CHANNEL_ACCESS_TOKEN)}`,
    },
    payload: JSON.stringify({ replyToken, messages }),
    muteHttpExceptions: true,
  });
}

function _isTriggered(event: LineWebhookEvent): boolean {
  if (isBotMentioned(event)) return true;
  const text = event.message?.text || '';
  return text.trimStart().startsWith('皮皮');
}

function _stripTrigger(text: string): string {
  const noMention = text.replace(/@\S+\s*/g, '').trim();
  return noMention.startsWith('皮皮') ? noMention.slice(2).trim() : noMention;
}

function _dispatch(text: string, replyToken: string): void {
  if (text.includes('目標價')) {
    const result = queryTargetPriceByCategory(text);
    if (result !== null) {
      _replyWithHelp(replyToken, result);
      return;
    }
    const single = queryTargetPriceSingle(text);
    if (single !== null) {
      _replyWithHelp(replyToken, single);
      return;
    }
  }

  if (/p\/?e/i.test(text)) {
    const result = queryPeerPeByCategory(text);
    if (result !== null) {
      _replyWithHelp(replyToken, result);
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
  _replyWithHelp(replyToken, `${a} ${b}`);
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
