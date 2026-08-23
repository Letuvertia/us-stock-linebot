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
  `皮皮聽得懂的指令："皮皮！... (產業/個股) + (報告分類)"\n` +
  `──────────────\n` +
  `產業：公用事業/原材料/工業/房地產/核心消費/能源/資訊科技/通訊服務/醫療保健/金融/非核心消費\n` +
  `個股：直接問股票代碼、英文或中文\n` +
  `報告分類：目標價、P/E、新聞、問題 (CFA刷題)`;

function _helpText(): string {
  const header = HELP_HEADERS[Math.floor(Math.random() * HELP_HEADERS.length)];
  return `${header}\n${HELP_BODY}`;
}

function _replyWithHelp(replyToken: string, mainText: string): void {
  const helpChunk = { type: 'text', text: _helpText() };
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
  const text = (event.message?.text || '').trim();
  return text.startsWith('皮皮') || /^(?:問題|題目|考題|cfa|quiz)/i.test(text);
}

function _stripTrigger(text: string): string {
  const noMention = text.replace(/@\S+\s*/g, '').trim();
  return noMention.startsWith('皮皮') ? noMention.slice(2).trim() : noMention;
}

function _dispatch(text: string, replyToken: string, userId?: string): void {
  const fnName = '_dispatch';

  // 1. CFA Learning Status Feedback: "皮皮 CFA 學習狀態回報 V1 M1 Ex1 我會 / 我不會"
  const feedbackMatch = /^CFA\s*學習狀態回報\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)\s*(我會|我不會)/i.exec(text);
  if (feedbackMatch) {
    const vol = parseInt(feedbackMatch[1], 10);
    const mod = parseInt(feedbackMatch[2], 10);
    const typeCode = feedbackMatch[3];
    const num = parseInt(feedbackMatch[4], 10);
    const isKnown = feedbackMatch[5] === '我會';
    logInfo(fnName, `CFA feedback match: V${vol} M${mod} ${typeCode}${num} (${feedbackMatch[5]})`);
    const res = updateCfaLearningFeedback(vol, mod, typeCode, num, isKnown, userId);

    // Automatically fetch next question by priority!
    const nextQ = fetchNextCfaQuestion(userId);
    if (nextQ) {
      sendReplyMessages(replyToken, [
        { type: 'text', text: res.message },
        nextQ.flexMessage,
      ]);
    } else {
      sendReplyMessage(replyToken, res.message);
    }
    return;
  }

  // 2. CFA Explanation Request: "皮皮 CFA 解析 V1 M1 Ex1"
  const solutionMatch = /^CFA\s*解析\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)/i.exec(text);
  if (solutionMatch) {
    const vol = parseInt(solutionMatch[1], 10);
    const mod = parseInt(solutionMatch[2], 10);
    const typeCode = solutionMatch[3];
    const num = parseInt(solutionMatch[4], 10);
    logInfo(fnName, `CFA explanation match: V${vol} M${mod} ${typeCode}${num}`);
    const solutionFlex = fetchCfaSolutionByRef(vol, mod, typeCode, num);
    if (solutionFlex) {
      sendReplyFlexMessage(replyToken, solutionFlex);
      return;
    } else {
      sendReplyMessage(replyToken, `🐶 找不到 V${vol} M${mod} ${typeCode}${num} 的解答喔！`);
      return;
    }
  }

  // 3. CFA Answer Submission: "皮皮 CFA 回答 V1 M1 Ex1 A"
  const answerMatch = /^CFA\s*回答\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)\s*([A-Ca-c])/i.exec(text);
  if (answerMatch) {
    const vol = parseInt(answerMatch[1], 10);
    const mod = parseInt(answerMatch[2], 10);
    const typeCode = answerMatch[3];
    const num = parseInt(answerMatch[4], 10);
    const chosen = answerMatch[5].toUpperCase();
    logInfo(fnName, `CFA answer match: V${vol} M${mod} ${typeCode}${num} -> ${chosen}`);
    const res = handleCfaAnswerSubmission(vol, mod, typeCode, num, chosen);
    if (res) {
      sendReplyFlexMessage(replyToken, res.flexMessage);
      return;
    } else {
      sendReplyMessage(replyToken, `🐶 找不到 V${vol} M${mod} ${typeCode}${num} 的題目資訊喔！`);
      return;
    }
  }

  // 4. CFA Question Request: user says "皮皮" + contains "CFA" and "問題/題目", e.g. "皮皮給我一個CFA題目！"
  const isCfa = /cfa/i.test(text);
  const isQuestion = /(?:問題|題目|考題|quiz)/i.test(text);
  if (isCfa && isQuestion) {
    logInfo(fnName, `CFA question request match for user ${userId || 'unknown'}`);
    const nextQ = fetchNextCfaQuestion(userId);
    if (nextQ) {
      sendReplyFlexMessage(replyToken, nextQ.flexMessage);
      return;
    } else {
      sendReplyMessage(replyToken, '🐶 目前題庫中沒有找到題目，請稍後再試！');
      return;
    }
  }

  if (/持股|持倉|倉位|部位/.test(text)) {
    logInfo(fnName, '持股 match — sending portfolio reply');
    executePortfolioReport(undefined, replyToken);
    return;
  }

  const recentNewsMatch = /最近([1-3一二三]?)天/.exec(text);
  if (recentNewsMatch && text.includes('新聞')) {
    const charMap: Record<string, number> = { '1': 1, '2': 2, '3': 3, '一': 1, '二': 2, '三': 3 };
    const days = charMap[recentNewsMatch[1]] ?? 1;
    const result = queryRecentNewsSummaries(days);
    if (result !== null) {
      logInfo(fnName, `最近${days}天新聞 match (${result.length} chars) — sending reply`);
      sendReplyMessage(replyToken, `${result}\n\n${_helpText()}`);
    } else {
      sendReplyMessage(replyToken, `📰 最近 ${days} 天內無新聞摘要\n\n${_helpText()}`);
    }
    return;
  }

  if (text.includes('新聞')) {
    const result = queryNewsByTicker(text);
    if (result !== null) {
      logInfo(fnName, `新聞 match (${result.length} chars) — sending reply`);
      sendReplyMessage(replyToken, `${result}\n\n${_helpText()}`);
      return;
    }
    if (/最近/.test(text)) {
      const recent = queryRecentNewsSummaries(1);
      if (recent !== null) {
        logInfo(fnName, `最近新聞 fallback — sending 1d reply`);
        sendReplyMessage(replyToken, `${recent}\n\n${_helpText()}`);
      } else {
        sendReplyMessage(replyToken, `📰 最近 1 天內無新聞摘要\n\n${_helpText()}`);
      }
      return;
    }
    logWarn(fnName, `新聞 branch: no ticker match for: ${truncate(text, 80)}`);
  }

  if (text.includes('目標價')) {
    const categoryResult = queryTargetPriceByCategory(text);
    if (categoryResult !== null) {
      logInfo(fnName, `目標價 category match (${categoryResult.length} chars) — sending reply`);
      sendReplyMessage(replyToken, `${categoryResult}\n\n${_helpText()}`);
      return;
    }
    const single = queryTargetPriceSingle(text);
    if (single !== null) {
      logInfo(fnName, `目標價 single match (${single.length} chars) — sending reply`);
      sendReplyMessage(replyToken, `${single}\n\n${_helpText()}`);
      return;
    }
    logWarn(fnName, `目標價 branch: no match for: ${truncate(text, 80)}`);
  }

  if (/p\/?e/i.test(text)) {
    const result = queryPeerPeByCategory(text);
    if (result !== null) {
      logInfo(fnName, `P/E match (${result.length} chars) — sending reply`);
      sendReplyMessage(replyToken, `${result}\n\n${_helpText()}`);
      return;
    }
    logWarn(fnName, `P/E branch: no category match for: ${truncate(text, 80)}`);
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

function handleChatWebhook(e: GoogleAppsScript.Events.DoPost): GoogleAppsScript.Content.TextOutput {
  const fnName = 'handleChatWebhook';

  try {
    const events = parseWebhookEvents(e);

    for (const event of events) {
      if (event.source?.groupId) {
        logInfo(fnName, `Group ID: ${event.source.groupId}`);
      }
      if (event.source?.userId) {
        logInfo(fnName, `User ID: ${event.source.userId}`);
      }

      if (event.type !== 'message' || event.message?.type !== 'text') continue;
      if (!_isTriggered(event)) continue;

      const userMessage = _stripTrigger(event.message?.text || '');
      if (!userMessage) continue;

      logInfo(fnName, `Received query from user ${event.source?.userId || 'unknown'}: ${truncate(userMessage, 50)}`);

      if (event.replyToken) {
        _dispatch(userMessage, event.replyToken, event.source?.userId);
      }
    }
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    logError(fnName, 'Webhook processing failed', error);
  }

  return ContentService.createTextOutput('OK').setMimeType(ContentService.MimeType.TEXT);
}
