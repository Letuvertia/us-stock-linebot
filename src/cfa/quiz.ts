/**
 * CFA Level 1 Interactive Quiz Module for LINE Bot
 * Fetches dynamic question cards, solutions, evaluates answers,
 * delivers textbook summaries, and tracks individual learning progress (Nuo and Niu) via Google Sheets.
 */

interface CfaQuestionRecord {
  rowIndex: number;       // Sheet 1-indexed row number
  module: string;         // "m01"
  moduleName: string;     // "Rates and Returns"
  type: string;           // "Example" | "Problem"
  number: number;         // 1, 2, ...
  answer: string;         // "A" | "B" | "C" | ""
  nuoAnswered: number;
  nuoCorrect: number;
  niuAnswered: number;
  niuCorrect: number;
  questionJson: string;   // Full Flex message JSON {"type":"flex", ...}
  solutionJson: string;   // Full Flex message JSON {"type":"flex", ...}
}

interface UserLearningModule {
  volume: string;         // "v01"
  volumeName: string;     // "Quantitative Methods"
  module: string;         // "m01"
  moduleName: string;     // "Rates and Returns"
  isNiuLearned: boolean;
  isNuoLearned: boolean;
}

type CfaUser = 'Nuo' | 'Niu';

const DEFAULT_CFA_SPREADSHEET_ID = '1uHIp7LFpbol8V1qFlptvX0HYQuSp5bLgjYAZztmONHY';
const DEFAULT_CFA_SUMMARY_SPREADSHEET_ID = '1hBVc__SUK9j3G3FCCCv6GNQSuNmT8S3inHTNcOLe76c';
const DEFAULT_CFA_TAB = 'v01-quantitative-methods';
const USER_LEARNING_PROGRESS_TAB = 'UserLearningProgress';

/**
 * Opens the CFA Question Bank spreadsheet.
 */
function _getCfaSpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
  const id = getScriptProperty(PROP_KEYS.CFA_QUESTION_SPREADSHEET_ID) || DEFAULT_CFA_SPREADSHEET_ID;
  return SpreadsheetApp.openById(id);
}

/**
 * Opens the CFA Module Summary / Learning History spreadsheet.
 */
function _getCfaSummarySpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
  const id = getScriptProperty(PROP_KEYS.CFA_SUMMARY_SPREADSHEET_ID) || DEFAULT_CFA_SUMMARY_SPREADSHEET_ID;
  return SpreadsheetApp.openById(id);
}

/**
 * Resolves whether the user is Nuo or Niu based on userId.
 */
function resolveCfaUser(userId?: string): CfaUser {
  if (!userId) return 'Nuo';
  const niuId = getScriptProperty(PROP_KEYS.NIU_USER_ID);
  if (niuId && userId === niuId) {
    return 'Niu';
  }
  return 'Nuo';
}

/**
 * Loads the user learning history table from UserLearningProgress tab.
 */
function loadUserLearningModules(): UserLearningModule[] {
  try {
    const sheet = _getCfaSummarySpreadsheet().getSheetByName(USER_LEARNING_PROGRESS_TAB);
    if (!sheet || sheet.getLastRow() <= 1) return [];

    const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 6).getValues();
    const modules: UserLearningModule[] = [];

    for (const row of rawRows) {
      const vol = String(row[0] || '').trim();
      const mod = String(row[2] || '').trim().toLowerCase();
      if (!vol || !mod) continue;

      const niuVal = String(row[4] || '').trim().toUpperCase();
      const nuoVal = String(row[5] || '').trim().toUpperCase();

      const isNiu = niuVal === 'TRUE' || niuVal === '1' || niuVal === 'CHECKED';
      const isNuo = nuoVal === 'TRUE' || nuoVal === '1' || nuoVal === 'CHECKED';

      modules.push({
        volume: vol,
        volumeName: String(row[1] || '').trim(),
        module: mod,
        moduleName: String(row[3] || '').trim(),
        isNiuLearned: isNiu,
        isNuoLearned: isNuo,
      });
    }
    return modules;
  } catch (err) {
    logError('loadUserLearningModules', `Failed to load UserLearningProgress: ${err}`);
    return [];
  }
}

/**
 * Gets list of learned module codes (e.g. ['m01', 'm02']) for the user.
 */
function getLearnedModuleCodes(user: CfaUser): string[] {
  const allModules = loadUserLearningModules();
  return allModules
    .filter(m => (user === 'Niu' ? m.isNiuLearned : m.isNuoLearned))
    .map(m => m.module.toLowerCase());
}

/**
 * Checks if the specified module is marked as learned by the user in UserLearningProgress.
 */
function isModuleLearnedByUser(vol: number, mod: number, userId?: string): boolean {
  const user = resolveCfaUser(userId);
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const learned = getLearnedModuleCodes(user);
  return learned.includes(modCode);
}

/**
 * Loads all CFA questions from the specified volume tab.
 */
function loadCfaQuestionsFromSheet(tabName: string = DEFAULT_CFA_TAB): CfaQuestionRecord[] {
  const sheet = _getCfaSpreadsheet().getSheetByName(tabName);
  if (!sheet || sheet.getLastRow() <= 1) return [];

  const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 11).getValues();
  const records: CfaQuestionRecord[] = [];

  for (let i = 0; i < rawRows.length; i++) {
    const row = rawRows[i];
    const mod = String(row[0] || '').trim();
    if (!mod) continue;

    records.push({
      rowIndex: i + 2,
      module: mod,
      moduleName: String(row[1] || '').trim(),
      type: String(row[2] || '').trim(),
      number: Number(row[3]) || (i + 1),
      answer: String(row[4] || '').trim().toUpperCase(),
      nuoAnswered: Number(row[5]) || 0,
      nuoCorrect: Number(row[6]) || 0,
      niuAnswered: Number(row[7]) || 0,
      niuCorrect: Number(row[8]) || 0,
      questionJson: String(row[9] || '').trim(),
      solutionJson: String(row[10] || '').trim(),
    });
  }

  return records;
}

/**
 * Selects the next CFA question based on learning progress:
 * - If vol and mod are specified (e.g. V1 M1), only chooses from that module.
 * - Otherwise, chooses from all modules marked as learned in UserLearningProgress.
 * - If user has no learned modules yet and no module specified, returns null.
 * - Prioritizes questions with 0 answers, then lowest accuracy ratio.
 */
function fetchNextCfaQuestion(
  userId?: string,
  targetVol?: number,
  targetMod?: number,
  tabName: string = DEFAULT_CFA_TAB
): { record: CfaQuestionRecord; flexMessage: object } | null {
  const allRecords = loadCfaQuestionsFromSheet(tabName);
  if (allRecords.length === 0) return null;

  const user = resolveCfaUser(userId);

  let candidateRecords: CfaQuestionRecord[] = [];
  if (targetMod !== undefined && targetMod > 0) {
    const modCode = `m${targetMod < 10 ? '0' + targetMod : targetMod}`;
    candidateRecords = allRecords.filter(r => r.module.toLowerCase() === modCode);
  } else {
    const learnedModules = getLearnedModuleCodes(user);
    if (learnedModules.length === 0) {
      return null;
    }
    candidateRecords = allRecords.filter(r => learnedModules.includes(r.module.toLowerCase()));
  }

  if (candidateRecords.length === 0) {
    return null;
  }

  // 1. Check for un-answered questions
  const unAnswered = candidateRecords.filter(r => {
    const ansCount = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
    return ansCount === 0;
  });

  let selected: CfaQuestionRecord;
  if (unAnswered.length > 0) {
    selected = unAnswered[0];
  } else {
    // 2. Sort by accuracy ratio ascending (lowest correct ratio first)
    const sorted = [...candidateRecords].sort((a, b) => {
      const aAns = user === 'Niu' ? a.niuAnswered : a.nuoAnswered;
      const aCor = user === 'Niu' ? a.niuCorrect : a.nuoCorrect;
      const bAns = user === 'Niu' ? b.niuAnswered : b.nuoAnswered;
      const bCor = user === 'Niu' ? b.niuCorrect : b.nuoCorrect;

      const aRatio = aAns > 0 ? aCor / aAns : 0;
      const bRatio = bAns > 0 ? bCor / bAns : 0;

      if (aRatio !== bRatio) return aRatio - bRatio;
      return aAns - bAns;
    });
    selected = sorted[0];
  }

  try {
    const flexMessage = JSON.parse(selected.questionJson);
    return { record: selected, flexMessage };
  } catch (err) {
    logError('fetchNextCfaQuestion', `Failed to parse question JSON: ${err}`);
    return null;
  }
}

/**
 * Finds a specific question record by Volume, Module, Type (Ex/Pr), and Number.
 */
function findCfaQuestionByRef(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  tabName: string = DEFAULT_CFA_TAB
): CfaQuestionRecord | null {
  const records = loadCfaQuestionsFromSheet(tabName);
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const isExample = /^(?:ex|example)$/i.test(typeCode.trim());
  const typeName = isExample ? 'Example' : 'Problem';

  for (const r of records) {
    if (r.module.toLowerCase() === modCode && r.type === typeName && r.number === num) {
      return r;
    }
  }
  return null;
}

/**
 * Evaluates user answer against correct answer in the sheet and returns the solution card
 * with an evaluation banner inserted at the top.
 */
function handleCfaAnswerSubmission(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  chosenOption: string,
  tabName: string = DEFAULT_CFA_TAB
): { isCorrect: boolean; flexMessage: object } | null {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record || !record.solutionJson) return null;

  const chosen = chosenOption.trim().toUpperCase();
  const isCorrect = record.answer ? chosen === record.answer : false;

  const bannerText = isCorrect
    ? `🎉 答對了！✅ 正確答案是 ${record.answer}`
    : `❌ 答錯囉（你選了 ${chosen}）✅ 正確答案是 ${record.answer}`;

  try {
    const flexMessage: any = JSON.parse(record.solutionJson);
    const bubble = flexMessage.type === 'flex' ? flexMessage.contents : flexMessage;

    // Insert banner card at the very top of body contents
    const bannerBox = {
      type: 'box',
      layout: 'vertical',
      backgroundColor: isCorrect ? '#E8F5E9' : '#FFEBEE',
      paddingAll: 'md',
      cornerRadius: 'md',
      contents: [
        {
          type: 'text',
          text: bannerText,
          weight: 'bold',
          size: 'sm',
          color: isCorrect ? '#2E7D32' : '#C62828',
          wrap: true,
        },
      ],
    };

    if (bubble.body && Array.isArray(bubble.body.contents)) {
      bubble.body.contents.unshift(bannerBox, { type: 'separator', margin: 'md' });
    }

    return { isCorrect, flexMessage };
  } catch (err) {
    logError('handleCfaAnswerSubmission', `Failed to parse solution JSON: ${err}`);
    return null;
  }
}

/**
 * Retrieves the solution card for open-ended or directly requested explanations.
 */
function fetchCfaSolutionByRef(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  tabName: string = DEFAULT_CFA_TAB
): object | null {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record || !record.solutionJson) return null;

  try {
    return JSON.parse(record.solutionJson);
  } catch (err) {
    logError('fetchCfaSolutionByRef', `Failed to parse solution JSON: ${err}`);
    return null;
  }
}

const CFA_FEEDBACK_HEADERS = [
  '皮皮把你的答案卷放到廢紙回收箱',
  '皮皮不喜歡考試',
  '阿猩覺得這一題答案是4',
  '皮皮剛剛撞到桌角',
  '皮皮打了一個哈欠',
  '皮皮在抓癢',
  '皮皮發現廚房有煙燻魷魚絲',
  '阿猩在認真地把CFA課本每一頁都對摺',
];

/**
 * Builds the cumulative progress text lines for learned modules (e.g. "V1 / LM1 - Rates and Returns: 2/28").
 */
function _buildCumulativeProgressLines(user: CfaUser): string[] {
  const allLearningModules = loadUserLearningModules();
  const learnedModules = allLearningModules.filter(m => (user === 'Niu' ? m.isNiuLearned : m.isNuoLearned));
  const allQuestions = loadCfaQuestionsFromSheet(DEFAULT_CFA_TAB);
  const moduleLines: string[] = [];

  for (const lm of learnedModules) {
    const modCode = lm.module.toLowerCase();
    const modQuestions = allQuestions.filter(q => q.module.toLowerCase() === modCode);
    const totalCount = modQuestions.length;

    if (totalCount === 0) continue;

    const correctCount = modQuestions.filter(q => {
      const cor = user === 'Niu' ? q.niuCorrect : q.nuoCorrect;
      return cor > 0;
    }).length;

    const isAllCorrect = totalCount > 0 && correctCount >= totalCount;
    const volNum = parseInt(lm.volume.replace(/\D/g, ''), 10) || 1;
    const modNum = parseInt(lm.module.replace(/\D/g, ''), 10) || 1;

    const checkEmoji = isAllCorrect ? ' ✅' : '';
    moduleLines.push(`V${volNum} / LM${modNum} - ${lm.moduleName}: ${correctCount}/${totalCount}${checkEmoji}`);
  }

  if (moduleLines.length === 0) {
    moduleLines.push(`(None yet)`);
  }

  return moduleLines;
}

/**
 * Checks if all questions in the specified module have been answered correctly by the user.
 */
function checkIsModuleCompleted(
  user: CfaUser,
  vol: number,
  mod: number,
  tabName: string = DEFAULT_CFA_TAB
): boolean {
  const allQuestions = loadCfaQuestionsFromSheet(tabName);
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const modQuestions = allQuestions.filter(q => q.module.toLowerCase() === modCode);
  const totalCount = modQuestions.length;
  if (totalCount === 0) return false;

  const correctCount = modQuestions.filter(q => {
    const cor = user === 'Niu' ? q.niuCorrect : q.nuoCorrect;
    return cor > 0;
  }).length;

  return correctCount >= totalCount;
}

/**
 * Updates individual user learning status in Google Sheets when user clicks
 * "我看懂啦，下一題" (isKnown=true) or "我沒看懂，算了下一題" (isKnown=false).
 */
function updateCfaLearningFeedback(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  isKnown: boolean,
  userId?: string,
  tabName: string = DEFAULT_CFA_TAB
): { success: boolean; user: CfaUser; record: CfaQuestionRecord; message: string } {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record) {
    return {
      success: false,
      user: 'Nuo',
      record: {} as CfaQuestionRecord,
      message: `找不到題目 V${vol} M${mod} ${typeCode}${num}`,
    };
  }

  const user = resolveCfaUser(userId);
  const sheet = _getCfaSpreadsheet().getSheetByName(tabName);
  if (!sheet) {
    return { success: false, user, record, message: `找不到試算表分頁 ${tabName}` };
  }

  // Nuo: Col 6 (Answered), Col 7 (Correct)
  // Niu: Col 8 (Answered), Col 9 (Correct)
  const ansCol = user === 'Niu' ? 8 : 6;
  const corCol = user === 'Niu' ? 9 : 7;

  const currentAns = user === 'Niu' ? record.niuAnswered : record.nuoAnswered;
  const currentCor = user === 'Niu' ? record.niuCorrect : record.nuoCorrect;

  const newAns = currentAns + 1;
  const newCor = isKnown ? currentCor + 1 : currentCor;

  sheet.getRange(record.rowIndex, ansCol).setValue(newAns);
  sheet.getRange(record.rowIndex, corCol).setValue(newCor);

  const typeDisplay = record.type === 'Example' ? 'Example' : 'Problem';
  const statusDisplay = isKnown ? '✅ 我看懂了（正確次數 +1）' : '📖 還沒看懂（保留複習）';
  const header = CFA_FEEDBACK_HEADERS[Math.floor(Math.random() * CFA_FEEDBACK_HEADERS.length)];
  const progressLines = _buildCumulativeProgressLines(user);

  const message =
    `${header}\n` +
    `──────────────\n` +
    `題目：${record.moduleName} - ${typeDisplay} ${record.number}\n` +
    `狀態：${statusDisplay}\n` +
    `累積練習題目：\n  ` +
    progressLines.join('\n  ');

  return { success: true, user, record, message };
}

/**
 * Updates module-level learning status in UserLearningProgress tab (e.g. V1 M1 -> TRUE).
 */
function updateCfaModuleLearningProgress(
  vol: number,
  mod: number,
  isLearned: boolean = true,
  userId?: string
): { success: boolean; user: CfaUser; message: string } {
  const user = resolveCfaUser(userId);
  const sheet = _getCfaSummarySpreadsheet().getSheetByName(USER_LEARNING_PROGRESS_TAB);
  if (!sheet || sheet.getLastRow() <= 1) {
    return { success: false, user, message: `找不到進度表 ${USER_LEARNING_PROGRESS_TAB}` };
  }

  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 6).getValues();
  const col = user === 'Niu' ? 5 : 6;

  for (let i = 0; i < rawRows.length; i++) {
    const rowMod = String(rawRows[i][2] || '').trim().toLowerCase();
    if (rowMod === modCode) {
      const rowIndex = i + 2;
      sheet.getRange(rowIndex, col).setValue(isLearned ? 'TRUE' : '');
      return {
        success: true,
        user,
        message: `🐶 已更新 ${user} 的學習進度：V${vol} M${mod} (${rawRows[i][3]}) 已標記為已學習！`,
      };
    }
  }
  return { success: false, user, message: `找不到模組 V${vol} M${mod}` };
}

/**
 * Builds the Module Completion Celebration Flex Card.
 */
function _buildCfaModuleCompletedFlexCard(
  user: CfaUser,
  vol: number,
  mod: number,
  moduleName: string
): object {
  const nextMod = mod + 1;
  const progressLines = _buildCumulativeProgressLines(user);
  const lineContents = progressLines.map(line => ({
    type: 'text',
    text: line,
    size: 'sm',
    color: '#333333',
    wrap: true,
  }));

  return {
    type: 'flex',
    altText: `🎉 Congratulations! LM${mod} ${moduleName} Finished!`,
    contents: {
      type: 'bubble',
      size: 'mega',
      header: {
        type: 'box',
        layout: 'vertical',
        backgroundColor: '#0D2538',
        paddingAll: 'lg',
        contents: [
          {
            type: 'text',
            text: `📖 CFA LEVEL 1 · VOL ${vol}`,
            weight: 'bold',
            color: '#00C853',
            size: 'xs',
          },
          {
            type: 'text',
            text: `🎉 Congratulations! LM${mod} ${moduleName} Finished!`,
            weight: 'bold',
            color: '#FFFFFF',
            size: 'md',
            margin: 'xs',
            wrap: true,
          },
        ],
      },
      body: {
        type: 'box',
        layout: 'vertical',
        paddingAll: 'lg',
        spacing: 'md',
        contents: [
          {
            type: 'text',
            text: 'Modules Learned:',
            weight: 'bold',
            size: 'sm',
            color: '#111111',
          },
          {
            type: 'box',
            layout: 'vertical',
            spacing: 'sm',
            contents: lineContents,
          },
          {
            type: 'separator',
            margin: 'lg',
          },
          {
            type: 'button',
            style: 'primary',
            color: '#00C853',
            height: 'sm',
            action: {
              type: 'message',
              label: '下一單元',
              text: `皮皮 CFA 課本摘要 V${vol} M${nextMod}`,
            },
          },
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'message',
              label: '練習其他單元的題目',
              text: '皮皮 CFA 題目',
            },
          },
        ],
      },
    },
  };
}

/**
 * Builds the 3-button navigation Flex Card after reading a module summary.
 */
function _buildCfaModuleSummaryActionCard(vol: number, mod: number): object {
  const nextMod = mod + 1;
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const allLearningModules = loadUserLearningModules();
  const lm = allLearningModules.find(m => m.module.toLowerCase() === modCode);
  const moduleName = lm ? lm.moduleName : 'Rates and Returns';

  return {
    type: 'flex',
    altText: `📖 CFA LEVEL 1 · VOL ${vol} · LM${mod} ${moduleName}`,
    contents: {
      type: 'bubble',
      size: 'mega',
      header: {
        type: 'box',
        layout: 'vertical',
        backgroundColor: '#0D2538',
        paddingAll: 'lg',
        contents: [
          {
            type: 'text',
            text: `📖 CFA LEVEL 1 · VOL ${vol}`,
            weight: 'bold',
            color: '#00C853',
            size: 'xs',
          },
          {
            type: 'text',
            text: `LM${mod}: ${moduleName}`,
            weight: 'bold',
            color: '#FFFFFF',
            size: 'md',
            margin: 'xs',
            wrap: true,
          },
        ],
      },
      body: {
        type: 'box',
        layout: 'vertical',
        paddingAll: 'lg',
        spacing: 'md',
        contents: [
          {
            type: 'button',
            style: 'primary',
            color: '#00C853',
            height: 'sm',
            action: {
              type: 'message',
              label: '我會啦，練習題目',
              text: `皮皮 CFA 學習狀態回報 V${vol} M${mod} && 皮皮 CFA 題目 V${vol} M${mod}`,
            },
          },
          {
            type: 'button',
            style: 'primary',
            color: '#0D2538',
            height: 'sm',
            action: {
              type: 'message',
              label: '我會啦，下一單元',
              text: `皮皮 CFA 學習狀態回報 V${vol} M${mod} && 皮皮 CFA 課本摘要 V${vol} M${nextMod}`,
            },
          },
          {
            type: 'button',
            style: 'secondary',
            height: 'sm',
            action: {
              type: 'message',
              label: '我有地方不懂，我要問AI',
              text: 'AI回答功能還沒開發',
            },
          },
        ],
      },
    },
  };
}

/**
 * Builds the single Flex Card for CFA Progress Summary.
 */
function _buildCfaProgressFlexCard(
  user: CfaUser,
  hasLearnedModules: boolean,
  nextVol: number,
  nextMod: number,
  progressLines: string[]
): object {
  const lineContents = progressLines.map(line => ({
    type: 'text',
    text: line,
    size: 'sm',
    color: '#333333',
    wrap: true,
  }));

  const buttonContents: object[] = [];
  if (!hasLearnedModules) {
    buttonContents.push({
      type: 'button',
      style: 'primary',
      color: '#0D2538',
      height: 'sm',
      action: {
        type: 'message',
        label: '開始第一單元',
        text: '皮皮 CFA 課本摘要 V1 M1',
      },
    });
  } else {
    buttonContents.push(
      {
        type: 'button',
        style: 'primary',
        color: '#0D2538',
        height: 'sm',
        action: {
          type: 'message',
          label: '下一單元',
          text: `皮皮 CFA 課本摘要 V${nextVol} M${nextMod}`,
        },
      },
      {
        type: 'button',
        style: 'secondary',
        height: 'sm',
        action: {
          type: 'message',
          label: '練習題目',
          text: '皮皮 CFA 題目',
        },
      }
    );
  }

  return {
    type: 'flex',
    altText: `📖 CFA LEVEL 1 · ${user}'s Learning Progress`,
    contents: {
      type: 'bubble',
      size: 'mega',
      header: {
        type: 'box',
        layout: 'vertical',
        backgroundColor: '#0D2538',
        paddingAll: 'lg',
        contents: [
          {
            type: 'text',
            text: '📖 CFA LEVEL 1',
            weight: 'bold',
            color: '#00C853',
            size: 'xs',
          },
          {
            type: 'text',
            text: `${user}'s Learning Progress`,
            weight: 'bold',
            color: '#FFFFFF',
            size: 'md',
            margin: 'xs',
            wrap: true,
          },
        ],
      },
      body: {
        type: 'box',
        layout: 'vertical',
        paddingAll: 'lg',
        spacing: 'md',
        contents: [
          {
            type: 'text',
            text: 'Modules Learned:',
            weight: 'bold',
            size: 'sm',
            color: '#111111',
          },
          {
            type: 'box',
            layout: 'vertical',
            spacing: 'sm',
            contents: lineContents,
          },
          {
            type: 'separator',
            margin: 'lg',
          },
          ...buttonContents,
        ],
      },
    },
  };
}

/**
 * Generates user learning progress report as a single standalone Flex Card.
 */
function getCfaUserProgressReport(userId?: string): object {
  const user = resolveCfaUser(userId);
  const allLearningModules = loadUserLearningModules();
  const learnedModules = allLearningModules.filter(m => (user === 'Niu' ? m.isNiuLearned : m.isNuoLearned));
  const hasLearned = learnedModules.length > 0;

  let nextVol = 1;
  let nextMod = 1;

  if (hasLearned) {
    const last = learnedModules[learnedModules.length - 1];
    nextVol = parseInt(last.volume.replace(/\D/g, ''), 10) || 1;
    const currentModNum = parseInt(last.module.replace(/\D/g, ''), 10) || 1;
    nextMod = currentModNum + 1;
  }

  const progressLines = _buildCumulativeProgressLines(user);
  return _buildCfaProgressFlexCard(user, hasLearned, nextVol, nextMod, progressLines);
}

/**
 * Fetches the textbook summary markdown for a given Volume and Module from CFA Module Summary sheet.
 */
function fetchCfaModuleSummary(vol: number, mod: number, tabName: string = DEFAULT_CFA_TAB): string | null {
  try {
    const sheet = _getCfaSummarySpreadsheet().getSheetByName(tabName);
    if (!sheet || sheet.getLastRow() <= 1) return null;

    const rows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 3).getValues();
    const modCode = `m${mod < 10 ? '0' + mod : mod}`;

    for (const r of rows) {
      if (String(r[0] || '').trim().toLowerCase() === modCode) {
        return String(r[2] || '').trim();
      }
    }
    return null;
  } catch (err) {
    logError('fetchCfaModuleSummary', `Failed to fetch module summary: ${err}`);
    return null;
  }
}
