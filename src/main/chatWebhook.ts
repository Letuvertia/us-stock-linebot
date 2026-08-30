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

function _executeSingleCommand(
  text: string,
  userId: string | undefined,
  outMessages: object[],
  isChained: boolean = false
): void {
  const fnName = '_executeSingleCommand';

  // 0. AI feature placeholder: "AI回答功能還沒開發"
  if (/AI回答功能還沒開發|問AI/i.test(text)) {
    logInfo(fnName, 'AI question placeholder matched');
    outMessages.push({
      type: 'text',
      text: '🤖 皮皮的 AI 解答功能還在開發中喔！敬請期待～',
    });
    return;
  }

  // 1. CFA Learning Status Feedback:
  // A. Question feedback: "皮皮 CFA 學習狀態回報 V1 M1 Ex1 我會 / 我不會" (optionally ending with 模式)
  const qFeedbackMatch = /^CFA\s*學習狀態回報\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)\s*(我會|我不會)/i.exec(text);
  if (qFeedbackMatch) {
    const vol = parseInt(qFeedbackMatch[1], 10);
    const mod = parseInt(qFeedbackMatch[2], 10);
    if (!isModuleLearnedByUser(vol, mod, userId)) {
      logInfo(fnName, `User ${userId || 'unknown'} has not learned V${vol} M${mod} yet — ignoring question feedback`);
      return;
    }

    const typeCode = qFeedbackMatch[3];
    const num = parseInt(qFeedbackMatch[4], 10);
    const isKnown = qFeedbackMatch[5] === '我會';
    const isDrill = /刷題/i.test(text);
    const isReview = /複習/i.test(text);
    const mode: CfaQuizMode = isDrill ? 'drill' : (isReview ? 'review' : 'learn');

    logInfo(fnName, `CFA question feedback match: V${vol} M${mod} ${typeCode}${num} (${qFeedbackMatch[5]}), mode=${mode}`);
    const res = updateCfaLearningFeedback(vol, mod, typeCode, num, isKnown, userId);
    outMessages.push({ type: 'text', text: res.message });

    if (mode === 'drill') {
      // In drill mode, always fetch next question randomly across all answered questions in learned modules
      const nextQ = fetchNextCfaQuestion(userId, undefined, undefined, DEFAULT_CFA_TAB, 'drill');
      if (nextQ) {
        outMessages.push(nextQ.flexMessage);
      }
    } else if (mode === 'review') {
      // In review mode, continuously fetch next question from the SAME module without celebration card
      const nextQ = fetchNextCfaQuestion(userId, vol, mod, DEFAULT_CFA_TAB, 'review');
      if (nextQ) {
        outMessages.push(nextQ.flexMessage);
      }
    } else {
      // In learn mode, if all questions in this module are completed by user, celebrate!
      if (checkIsModuleCompleted(res.user, vol, mod)) {
        const completedCard = _buildCfaModuleCompletedFlexCard(res.user, vol, mod, res.record.moduleName);
        outMessages.push(completedCard);
      } else {
        // Automatically fetch next question from the SAME module in learn mode
        const nextQ = fetchNextCfaQuestion(userId, vol, mod, DEFAULT_CFA_TAB, 'learn');
        if (nextQ) {
          outMessages.push(nextQ.flexMessage);
        }
      }
    }
    return;
  }

  // B. Module-level feedback: "皮皮 CFA 學習狀態回報 V1 M1"
  const mFeedbackMatch = /^CFA\s*學習狀態回報\s*V?(\d+)\s*M?(\d+)/i.exec(text);
  if (mFeedbackMatch) {
    const vol = parseInt(mFeedbackMatch[1], 10);
    const mod = parseInt(mFeedbackMatch[2], 10);
    logInfo(fnName, `CFA module feedback match: V${vol} M${mod}`);
    const res = updateCfaModuleLearningProgress(vol, mod, true, userId);
    if (!isChained) {
      outMessages.push({ type: 'text', text: res.message });
    }
    return;
  }

  // 2. CFA Explanation Request: "皮皮 CFA 解析 V1 M1 Ex1" (optionally ending with 模式)
  const solutionMatch = /^CFA\s*解析\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)/i.exec(text);
  if (solutionMatch) {
    const vol = parseInt(solutionMatch[1], 10);
    const mod = parseInt(solutionMatch[2], 10);
    if (!isModuleLearnedByUser(vol, mod, userId)) {
      logInfo(fnName, `User ${userId || 'unknown'} has not learned V${vol} M${mod} yet — ignoring solution request`);
      return;
    }

    const typeCode = solutionMatch[3];
    const num = parseInt(solutionMatch[4], 10);
    const isDrill = /刷題/i.test(text);
    const isReview = /複習/i.test(text);
    const mode: CfaQuizMode = isDrill ? 'drill' : (isReview ? 'review' : 'learn');
    logInfo(fnName, `CFA explanation match: V${vol} M${mod} ${typeCode}${num}, mode=${mode}`);
    const solutionFlex = fetchCfaSolutionByRef(vol, mod, typeCode, num, userId, mode);
    if (solutionFlex) {
      outMessages.push(solutionFlex);
    } else {
      outMessages.push({ type: 'text', text: `🐶 找不到 V${vol} M${mod} ${typeCode}${num} 的解答喔！` });
    }
    return;
  }

  // 3. CFA Answer Submission: "皮皮 CFA 回答 V1 M1 Ex1 A" (optionally ending with 模式)
  const answerMatch = /^CFA\s*回答\s*V?(\d+)\s*M?(\d+)\s*(Ex|Pr|Example|Problem)\s*(\d+)\s*([A-Ca-c])/i.exec(text);
  if (answerMatch) {
    const vol = parseInt(answerMatch[1], 10);
    const mod = parseInt(answerMatch[2], 10);
    if (!isModuleLearnedByUser(vol, mod, userId)) {
      logInfo(fnName, `User ${userId || 'unknown'} has not learned V${vol} M${mod} yet — ignoring answer submission`);
      return;
    }

    const typeCode = answerMatch[3];
    const num = parseInt(answerMatch[4], 10);
    const chosen = answerMatch[5].toUpperCase();
    const isDrill = /刷題/i.test(text);
    const isReview = /複習/i.test(text);
    const mode: CfaQuizMode = isDrill ? 'drill' : (isReview ? 'review' : 'learn');
    logInfo(fnName, `CFA answer match: V${vol} M${mod} ${typeCode}${num} -> ${chosen}, mode=${mode}`);
    const res = handleCfaAnswerSubmission(vol, mod, typeCode, num, chosen, userId, mode);
    if (res) {
      outMessages.push(res.flexMessage);
    } else {
      outMessages.push({ type: 'text', text: `🐶 找不到 V${vol} M${mod} ${typeCode}${num} 的題目資訊喔！` });
    }
    return;
  }

  // 4. CFA Module / Volume Selectors:
  // A. Continue learning incomplete modules
  if (/^CFA\s*(?:學習題目|學習某單元題目|繼續學習某單元題目|繼續學習單元題目)(?!\s*V?\d+)/i.test(text)) {
    logInfo(fnName, 'CFA continue learn quiz selector request');
    const selectorCard = buildCfaModuleSelectorFlexCard(userId, 'learn_quiz');
    outMessages.push(selectorCard);
    return;
  }

  // B. Summary menu: "皮皮 CFA 摘要選單", "皮皮 CFA 摘要選單 V1", "皮皮 CFA 查看單元摘要", "皮皮 CFA 複習摘要"
  const summaryMenuMatch = /^CFA\s*(?:摘要選單|查看單元摘要|複習摘要|摘要清單|摘要)(?:\s*V?(\d+))?$/i.exec(text.trim());
  if (summaryMenuMatch) {
    const volNum = summaryMenuMatch[1] ? parseInt(summaryMenuMatch[1], 10) : undefined;
    logInfo(fnName, `CFA summary menu request: vol=${volNum}`);
    if (volNum !== undefined && volNum > 0) {
      const selectorCard = buildCfaModuleSelectorForVolumeFlexCard(userId, volNum, 'summary');
      outMessages.push(selectorCard);
    } else {
      const selectorCard = buildCfaVolumeSelectorFlexCard(userId, 'summary');
      outMessages.push(selectorCard);
    }
    return;
  }

  // C. Quiz unlearned module confirmation: "皮皮 CFA 題目選單 V1 M2 確認"
  const quizConfirmMatch = /^CFA\s*題目選單\s*V?(\d+)\s*M?(\d+)\s*確認/i.exec(text.trim());
  if (quizConfirmMatch) {
    const volNum = parseInt(quizConfirmMatch[1], 10);
    const modNum = parseInt(quizConfirmMatch[2], 10);
    logInfo(fnName, `CFA unlearned module quiz confirm: V${volNum} M${modNum}`);
    const selectorCard = buildCfaUnlearnedModuleConfirmFlexCard(volNum, modNum);
    outMessages.push(selectorCard);
    return;
  }

  // D. Quiz menu: "皮皮 CFA 題目選單", "皮皮 CFA 題目選單 V1", "皮皮 CFA 練習單元題目", "皮皮 CFA 複習題目"
  const quizMenuMatch = /^CFA\s*(?:題目選單|練習單元題目|複習題目|題目清單)(?:\s*V?(\d+))?$/i.exec(text.trim());
  if (quizMenuMatch) {
    const volNum = quizMenuMatch[1] ? parseInt(quizMenuMatch[1], 10) : undefined;
    logInfo(fnName, `CFA quiz menu request: vol=${volNum}`);
    if (volNum !== undefined && volNum > 0) {
      const selectorCard = buildCfaModuleSelectorForVolumeFlexCard(userId, volNum, 'quiz');
      outMessages.push(selectorCard);
    } else {
      const selectorCard = buildCfaVolumeSelectorFlexCard(userId, 'quiz');
      outMessages.push(selectorCard);
    }
    return;
  }

  // 5. CFA Drill Mode Request: "皮皮 CFA 刷題模式" or "皮皮 CFA 刷題"
  if (/^CFA\s*(?:刷題模式|刷題)/i.test(text)) {
    logInfo(fnName, 'CFA drill mode request');
    const nextQ = fetchNextCfaQuestion(userId, undefined, undefined, DEFAULT_CFA_TAB, 'drill');
    if (nextQ) {
      outMessages.push(nextQ.flexMessage);
    } else {
      const user = resolveCfaUser(userId);
      const learnedModules = getLearnedModuleCodes(user);
      if (learnedModules.length === 0) {
        const startCard = getCfaUserProgressReport(userId);
        outMessages.push(startCard);
      } else {
        outMessages.push({
          type: 'text',
          text: '🐶 刷題模式會複習已練習過的題目！目前已學單元中尚未有練習紀錄，請先點選「複習單元題目」或從第一單元開始練習喔！'
        });
      }
    }
    return;
  }

  // 6. CFA Textbook Summary: "皮皮 CFA 課本摘要 V1 M1"
  const summaryMatch = /^CFA\s*(?:課本摘要|摘要|導讀)\s*V?(\d+)\s*M?(\d+)/i.exec(text);
  if (summaryMatch) {
    const vol = parseInt(summaryMatch[1], 10);
    const mod = parseInt(summaryMatch[2], 10);
    logInfo(fnName, `CFA summary match: V${vol} M${mod}`);
    const summaryText = fetchCfaModuleSummary(vol, mod);
    if (summaryText) {
      const chunks = splitLongMessage(summaryText);
      for (const c of chunks) {
        outMessages.push({ type: 'text', text: c });
      }
      const actionCard = _buildCfaModuleSummaryActionCard(vol, mod);
      outMessages.push(actionCard);
    } else {
      outMessages.push({ type: 'text', text: `🐶 找不到 V${vol} M${mod} 的課本摘要喔！` });
    }
    return;
  }

  // 7. CFA Question Request: "皮皮 CFA 題目 V1 M1 學習模式", "皮皮 CFA 題目 V1 M1 複習模式", or "皮皮 CFA 題目 V1 M1"
  const questionMatch = /^CFA\s*(?:題目|問題|考題|quiz)(?:\s*V?(\d+)\s*M?(\d+))?/i.exec(text);
  if (questionMatch) {
    const vol = questionMatch && questionMatch[1] ? parseInt(questionMatch[1], 10) : undefined;
    const mod = questionMatch && questionMatch[2] ? parseInt(questionMatch[2], 10) : undefined;
    const isDrill = /刷題/i.test(text);
    const isReview = /複習/i.test(text);
    const mode: CfaQuizMode = isDrill ? 'drill' : (isReview ? 'review' : 'learn');
    logInfo(fnName, `CFA question request match: vol=${vol} mod=${mod}, mode=${mode}`);
    const nextQ = fetchNextCfaQuestion(userId, vol, mod, DEFAULT_CFA_TAB, mode);
    if (nextQ) {
      outMessages.push(nextQ.flexMessage);
    } else {
      const user = resolveCfaUser(userId);
      const learnedModules = getLearnedModuleCodes(user);
      if (learnedModules.length === 0 && mod === undefined) {
        const startCard = getCfaUserProgressReport(userId);
        outMessages.push(startCard);
      } else if (mode === 'learn' && vol !== undefined && mod !== undefined && checkIsModuleCompleted(user, vol, mod)) {
        const allLearningModules = loadUserLearningModules();
        const modCode = `m${mod < 10 ? '0' + mod : mod}`;
        const lm = allLearningModules.find(m => m.module.toLowerCase() === modCode);
        const moduleName = lm ? lm.moduleName : `LM${mod}`;
        const completedCard = _buildCfaModuleCompletedFlexCard(user, vol, mod, moduleName);
        outMessages.push(completedCard);
      } else if (mode === 'drill') {
        outMessages.push({
          type: 'text',
          text: '🐶 刷題模式會複習已練習過的題目！目前已學單元中尚未有練習紀錄，請先點選「複習單元題目」或從第一單元開始練習喔！'
        });
      } else {
        outMessages.push({ type: 'text', text: '🐶 目前題庫中沒有找到符合的題目，請稍後再試！' });
      }
    }
    return;
  }

  // 8. CFA Progress Report: user says "皮皮 CFA 進度" or just "皮皮 CFA" / "皮皮 CFA！"
  const isCfa = /cfa/i.test(text);
  const isProgress = /(?:進度|學習進度|筆記本|進度報告)/i.test(text);
  const isPureCfa = /^CFA[\s!！~～?？]*$/i.test(text);
  if (isCfa && (isProgress || isPureCfa)) {
    logInfo(fnName, `CFA progress query for user ${userId || 'unknown'}`);
    const flexCard = getCfaUserProgressReport(userId);
    outMessages.push(flexCard);
    return;
  }

  // 7. Portfolio report
  if (/持股|持倉|倉位|部位/.test(text)) {
    logInfo(fnName, '持股 match — sending portfolio reply');
    // Handled directly via existing flow
    return;
  }

  // 8. Recent news
  const recentNewsMatch = /最近([1-3一二三]?)天/.exec(text);
  if (recentNewsMatch && text.includes('新聞')) {
    const charMap: Record<string, number> = { '1': 1, '2': 2, '3': 3, '一': 1, '二': 2, '三': 3 };
    const days = charMap[recentNewsMatch[1]] ?? 1;
    const result = queryRecentNewsSummaries(days);
    if (result !== null) {
      outMessages.push({ type: 'text', text: `${result}\n\n${_helpText()}` });
    } else {
      outMessages.push({ type: 'text', text: `📰 最近 ${days} 天內無新聞摘要\n\n${_helpText()}` });
    }
    return;
  }

  // 9. Single news / target price / PE
  if (text.includes('新聞')) {
    const result = queryNewsByTicker(text);
    if (result !== null) {
      outMessages.push({ type: 'text', text: `${result}\n\n${_helpText()}` });
      return;
    }
    if (/最近/.test(text)) {
      const recent = queryRecentNewsSummaries(1);
      if (recent !== null) {
        outMessages.push({ type: 'text', text: `${recent}\n\n${_helpText()}` });
      } else {
        outMessages.push({ type: 'text', text: `📰 最近 1 天內無新聞摘要\n\n${_helpText()}` });
      }
      return;
    }
  }

  if (text.includes('目標價')) {
    const categoryResult = queryTargetPriceByCategory(text);
    if (categoryResult !== null) {
      outMessages.push({ type: 'text', text: `${categoryResult}\n\n${_helpText()}` });
      return;
    }
    const single = queryTargetPriceSingle(text);
    if (single !== null) {
      outMessages.push({ type: 'text', text: `${single}\n\n${_helpText()}` });
      return;
    }
  }

  if (/p\/?e/i.test(text)) {
    const result = queryPeerPeByCategory(text);
    if (result !== null) {
      outMessages.push({ type: 'text', text: `${result}\n\n${_helpText()}` });
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
  outMessages.push({ type: 'text', text: `${a} ${b}\n${_helpText()}` });
}

function _dispatch(rawText: string, replyToken: string, userId?: string): void {
  const fnName = '_dispatch';

  // Handle portfolio special case
  if (/持股|持倉|倉位|部位/.test(rawText)) {
    logInfo(fnName, '持股 match — sending portfolio reply');
    executePortfolioReport(undefined, replyToken);
    return;
  }

  const accumulatedMessages: object[] = [];

  // Support multiple commands separated by &&
  if (rawText.includes('&&')) {
    const subCommands = rawText.split('&&').map(c => _stripTrigger(c)).filter(Boolean);
    logInfo(fnName, `Chained command execution: ${subCommands.length} subcommands`);
    for (let i = 0; i < subCommands.length; i++) {
      _executeSingleCommand(subCommands[i], userId, accumulatedMessages, true);
    }
  } else {
    _executeSingleCommand(rawText, userId, accumulatedMessages, false);
  }

  if (accumulatedMessages.length > 0) {
    sendReplyMessages(replyToken, accumulatedMessages);
  }
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
