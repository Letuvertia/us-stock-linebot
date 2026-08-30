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
  nuoLearned: number;
  niuAnswered: number;
  niuCorrect: number;
  niuLearned: number;
  questionJson: string;   // Full Flex message JSON {"type":"flex", ...}
  solutionJson: string;   // Full Flex message JSON {"type":"flex", ...}
}

interface UserLearningModule {
  volume: string;         // "v01"
  volumeName: string;     // "Quantitative Methods"
  module: string;         // "m01"
  moduleName: string;     // "Rates and Returns"
  numberOfExamples: number;
  numberOfProblems: number;
  isReady: boolean;
  isNiuLearned: boolean;
  isNuoLearned: boolean;
}

type CfaUser = 'Nuo' | 'Niu';
type CfaQuizMode = 'learn' | 'review' | 'drill';

const DEFAULT_CFA_SPREADSHEET_ID = '1uHIp7LFpbol8V1qFlptvX0HYQuSp5bLgjYAZztmONHY';
const DEFAULT_CFA_SUMMARY_SPREADSHEET_ID = '1hBVc__SUK9j3G3FCCCv6GNQSuNmT8S3inHTNcOLe76c';
const DEFAULT_CFA_TAB = 'v01-quantitative-methods';
const USER_LEARNING_PROGRESS_TAB = 'UserLearningProgress';

const CFA_VOLUME_TABS: Record<number, string> = {
  1: 'v01-quantitative-methods',
  2: 'v02-economics',
  3: 'v03-corporate-issuers',
  4: 'v04-financial-statement-analysis',
  5: 'v05-equity-investments',
  6: 'v06-fixed-income',
  7: 'v07-derivatives',
  8: 'v08-alternative-investments',
  9: 'v09-portfolio-management',
  10: 'v10-ethical-and-professional-standards',
};

/**
 * Maps volume number (1-10) to corresponding Google Sheet tab name.
 */
function getCfaVolumeTab(vol: number = 1): string {
  return CFA_VOLUME_TABS[vol] || DEFAULT_CFA_TAB;
}

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
 * Loads the user learning history table from UserLearningProgress tab (9 columns).
 */
function loadUserLearningModules(): UserLearningModule[] {
  try {
    const sheet = _getCfaSummarySpreadsheet().getSheetByName(USER_LEARNING_PROGRESS_TAB);
    if (!sheet || sheet.getLastRow() <= 1) return [];

    const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 9).getValues();
    const modules: UserLearningModule[] = [];

    for (const row of rawRows) {
      const vol = String(row[0] || '').trim().toLowerCase();
      const mod = String(row[2] || '').trim().toLowerCase();
      if (!vol || !mod) continue;

      const numEx = Number(row[4]) || 0;
      const numPr = Number(row[5]) || 0;
      const readyVal = String(row[6] || '').trim().toUpperCase();
      const isReady = readyVal === 'TRUE' || readyVal === '1';

      const niuVal = String(row[7] || '').trim().toUpperCase();
      const nuoVal = String(row[8] || '').trim().toUpperCase();

      const isNiu = niuVal === 'TRUE' || niuVal === '1' || niuVal === 'CHECKED';
      const isNuo = nuoVal === 'TRUE' || nuoVal === '1' || nuoVal === 'CHECKED';

      modules.push({
        volume: vol,
        volumeName: String(row[1] || '').trim(),
        module: mod,
        moduleName: String(row[3] || '').trim(),
        numberOfExamples: numEx,
        numberOfProblems: numPr,
        isReady,
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
 * Loads all CFA questions from the specified volume tab (13 columns).
 */
function loadCfaQuestionsFromSheet(tabName: string = DEFAULT_CFA_TAB): CfaQuestionRecord[] {
  const sheet = _getCfaSpreadsheet().getSheetByName(tabName);
  if (!sheet || sheet.getLastRow() <= 1) return [];

  const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 13).getValues();
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
      nuoLearned: Number(row[7]) || 0,
      niuAnswered: Number(row[8]) || 0,
      niuCorrect: Number(row[9]) || 0,
      niuLearned: Number(row[10]) || 0,
      questionJson: String(row[11] || '').trim(),
      solutionJson: String(row[12] || '').trim(),
    });
  }

  return records;
}

/**
 * Records an attempt (answered + 1, and correct + 1 if isCorrect) to Google Sheets.
 */
function recordCfaQuestionAttempt(
  record: CfaQuestionRecord,
  user: CfaUser,
  isCorrect: boolean,
  tabName: string = DEFAULT_CFA_TAB
): void {
  try {
    const sheet = _getCfaSpreadsheet().getSheetByName(tabName);
    if (!sheet) return;

    // Nuo: Col 6 (Answered), Col 7 (Correct)
    // Niu: Col 9 (Answered), Col 10 (Correct)
    const ansCol = user === 'Niu' ? 9 : 6;
    const corCol = user === 'Niu' ? 10 : 7;

    const currentAns = user === 'Niu' ? record.niuAnswered : record.nuoAnswered;
    const currentCor = user === 'Niu' ? record.niuCorrect : record.nuoCorrect;

    const newAns = currentAns + 1;
    const newCor = isCorrect ? currentCor + 1 : currentCor;

    sheet.getRange(record.rowIndex, ansCol).setValue(newAns);
    if (isCorrect) {
      sheet.getRange(record.rowIndex, corCol).setValue(newCor);
    }
  } catch (err) {
    logError('recordCfaQuestionAttempt', `Failed to record attempt: ${err}`);
  }
}

/**
 * Returns mode tag string for button text suffixes.
 */
function _modeToTag(mode: CfaQuizMode): string {
  if (mode === 'drill') return ' 刷題模式';
  if (mode === 'review') return ' 複習模式';
  return ' 學習模式';
}

/**
 * Injects mode suffix into question button action texts so session mode persists.
 */
function _injectQuizModeIntoQuestionFlex(flexMessage: any, mode: CfaQuizMode): any {
  const jsonStr = JSON.stringify(flexMessage);
  const tag = _modeToTag(mode);
  const updatedStr = jsonStr.replace(
    /("text":\s*"皮皮 CFA (?:回答|解析)[^"]*?)(?:\s*(?:刷題模式|複習模式|學習模式))?(")/g,
    `$1${tag}$2`
  );
  try {
    return JSON.parse(updatedStr);
  } catch (err) {
    return flexMessage;
  }
}

/**
 * Injects mode suffix into solution feedback button action texts.
 */
function _injectQuizModeIntoSolutionFlex(flexMessage: any, mode: CfaQuizMode): any {
  const jsonStr = JSON.stringify(flexMessage);
  const tag = _modeToTag(mode);
  const updatedStr = jsonStr.replace(
    /("text":\s*"皮皮 CFA 學習狀態回報[^"]*?)(?:\s*(?:刷題模式|複習模式|學習模式))?(")/g,
    `$1${tag}$2`
  );
  try {
    return JSON.parse(updatedStr);
  } catch (err) {
    return flexMessage;
  }
}

/**
 * Selects the next CFA question based on learning progress and active mode:
 * - 刷題模式 (mode='drill'): Chooses randomly from ANSWERED questions across all learned modules,
 *   prioritizing lowest accuracy (錯題優先), then lowest learned count.
 * - 複習模式 (mode='review'): Continuously serves questions from target module,
 *   prioritizing lowest accuracy (錯題優先), then lowest learned count, without stopping for celebration.
 * - 學習模式 (mode='learn'): First serves unseen questions sequentially, then unlearned questions,
 *   and stops for celebration when module is completed.
 */
function fetchNextCfaQuestion(
  userId?: string,
  targetVol?: number,
  targetMod?: number,
  tabName: string = DEFAULT_CFA_TAB,
  mode: CfaQuizMode = 'learn'
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

  let selected: CfaQuestionRecord;

  if (mode === 'drill') {
    // 1. Drill mode (刷題模式):
    // Scope: ONLY answered questions (Answered > 0) from learned modules. Exclude unseen questions.
    const answeredCandidates = candidateRecords.filter(r => {
      const ans = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
      return ans > 0;
    });

    if (answeredCandidates.length === 0) {
      return null;
    }

    // Priority:
    // 1. Lowest Correct count ascending (cor ascending) -> 錯題/少對優先
    // 2. 低熟悉度優先: Learned = 0 first, then lowest learned count ascending
    // 3. Lowest answered count ascending
    const scored = answeredCandidates.map(r => {
      const ans = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
      const cor = user === 'Niu' ? r.niuCorrect : r.nuoCorrect;
      const lrn = user === 'Niu' ? r.niuLearned : r.nuoLearned;
      return { record: r, cor, lrn, ans };
    });

    scored.sort((a, b) => {
      if (a.cor !== b.cor) return a.cor - b.cor;
      if (a.lrn !== b.lrn) return a.lrn - b.lrn;
      return a.ans - b.ans;
    });

    // Randomly pick one among top candidate tier sharing lowest cor and lowest lrn
    const bestCor = scored[0].cor;
    const bestLrn = scored[0].lrn;
    const topTier = scored.filter(s => s.cor === bestCor && s.lrn === bestLrn);
    const chosen = topTier[Math.floor(Math.random() * topTier.length)];
    selected = chosen.record;
  } else if (mode === 'review') {
    // 2. Review mode (複習模式):
    // Keeps giving questions continuously based on:
    // 1. Lowest Correct count ascending (cor ascending) -> 錯題/少對優先
    // 2. Lowest Learned count ascending -> 低熟悉度優先
    // 3. Lowest Answered count ascending
    const scored = candidateRecords.map(r => {
      const ans = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
      const cor = user === 'Niu' ? r.niuCorrect : r.nuoCorrect;
      const lrn = user === 'Niu' ? r.niuLearned : r.nuoLearned;
      return { record: r, cor, lrn, ans };
    });

    scored.sort((a, b) => {
      if (a.cor !== b.cor) return a.cor - b.cor;
      if (a.lrn !== b.lrn) return a.lrn - b.lrn;
      return a.ans - b.ans;
    });

    selected = scored[0].record;
  } else {
    // 3. Learn mode (學習模式):
    // 1. Unseen questions (Answered = 0) in sequential textbook order
    const unseen = candidateRecords.filter(r => {
      const ans = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
      return ans === 0;
    });

    if (unseen.length > 0) {
      selected = unseen[0];
    } else {
      // 2. Filter for remaining unlearned questions (Learned = 0)
      const unlearned = candidateRecords.filter(r => {
        const lrn = user === 'Niu' ? r.niuLearned : r.nuoLearned;
        return lrn === 0;
      });

      if (unlearned.length === 0) {
        // All questions in this module are already learned! (Learning mode completes)
        return null;
      }

      // Sort unlearned questions by: Lowest Correct count -> Lowest Answered count
      const scored = unlearned.map(r => {
        const ans = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
        const cor = user === 'Niu' ? r.niuCorrect : r.nuoCorrect;
        return { record: r, cor, ans };
      });

      scored.sort((a, b) => {
        if (a.cor !== b.cor) return a.cor - b.cor;
        return a.ans - b.ans;
      });

      selected = scored[0].record;
    }
  }

  try {
    let flexMessage = JSON.parse(selected.questionJson);
    flexMessage = _injectQuizModeIntoQuestionFlex(flexMessage, mode);
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
 * Evaluates user answer against correct answer in the sheet, records attempt stats in sheet,
 * and returns the solution card with an evaluation banner inserted at the top.
 */
function handleCfaAnswerSubmission(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  chosenOption: string,
  userId?: string,
  mode: CfaQuizMode = 'learn',
  tabName: string = DEFAULT_CFA_TAB
): { isCorrect: boolean; flexMessage: object } | null {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record || !record.solutionJson) return null;

  const chosen = chosenOption.trim().toUpperCase();
  const isCorrect = record.answer ? chosen === record.answer : false;
  const user = resolveCfaUser(userId);

  // Update Answered (+1) and Correct (+1 if correct) in Sheet
  recordCfaQuestionAttempt(record, user, isCorrect, tabName);

  const bannerText = isCorrect
    ? `🎉 答對了！✅ 正確答案是 ${record.answer}`
    : `❌ 答錯囉（你選了 ${chosen}）✅ 正確答案是 ${record.answer}`;

  try {
    let flexMessage: any = JSON.parse(record.solutionJson);
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

    flexMessage = _injectQuizModeIntoSolutionFlex(flexMessage, mode);

    return { isCorrect, flexMessage };
  } catch (err) {
    logError('handleCfaAnswerSubmission', `Failed to parse solution JSON: ${err}`);
    return null;
  }
}

/**
 * Retrieves the solution card for open-ended or directly requested explanations,
 * and records attempt stats (answered +1, correct +1) in Google Sheets.
 */
function fetchCfaSolutionByRef(
  vol: number,
  mod: number,
  typeCode: string,
  num: number,
  userId?: string,
  mode: CfaQuizMode = 'learn',
  tabName: string = DEFAULT_CFA_TAB
): object | null {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record || !record.solutionJson) return null;

  const user = resolveCfaUser(userId);
  // For explanation requests (解析): update answered (+1) and correct (+1)
  recordCfaQuestionAttempt(record, user, true, tabName);

  try {
    let flexMessage = JSON.parse(record.solutionJson);
    flexMessage = _injectQuizModeIntoSolutionFlex(flexMessage, mode);
    return flexMessage;
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
 * Builds the cumulative progress text lines for learned modules (e.g. "🚀 V1 / LM1 - Rates and Returns: 24/28").
 * Uses rocket emoji at the front for in-progress modules, and checkmark at the front for completed modules.
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

    const learnedCount = modQuestions.filter(q => {
      const lrn = user === 'Niu' ? q.niuLearned : q.nuoLearned;
      return lrn > 0;
    }).length;

    const isAllLearned = totalCount > 0 && learnedCount >= totalCount;
    const volNum = parseInt(lm.volume.replace(/\D/g, ''), 10) || 1;
    const modNum = parseInt(lm.module.replace(/\D/g, ''), 10) || 1;

    const prefix = isAllLearned ? '✅ ' : '🚀 ';
    moduleLines.push(`${prefix}V${volNum} / LM${modNum} - ${lm.moduleName}: ${learnedCount}/${totalCount}`);
  }

  if (moduleLines.length === 0) {
    moduleLines.push(`(None yet)`);
  }

  return moduleLines;
}

/**
 * Checks if all questions in the specified module have been learned by the user (learned > 0).
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

  const learnedCount = modQuestions.filter(q => {
    const lrn = user === 'Niu' ? q.niuLearned : q.nuoLearned;
    return lrn > 0;
  }).length;

  return learnedCount >= totalCount;
}

/**
 * Updates individual user learning status in Google Sheets when user clicks
 * "我看懂啦，下一題" (isKnown=true) or "我沒看懂，算了下一題" (isKnown=false).
 * Updates only the Learned column (if isKnown=true).
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

  // Nuo: Col 8 (Learned)
  // Niu: Col 11 (Learned)
  const lrnCol = user === 'Niu' ? 11 : 8;
  const currentLrn = user === 'Niu' ? record.niuLearned : record.nuoLearned;

  if (isKnown) {
    const newLrn = currentLrn + 1;
    sheet.getRange(record.rowIndex, lrnCol).setValue(newLrn);
  }

  const typeDisplay = record.type === 'Example' ? 'Example' : 'Problem';
  const statusDisplay = isKnown ? '✅ 我看懂了' : '📖 還沒看懂（保留複習）';
  const header = CFA_FEEDBACK_HEADERS[Math.floor(Math.random() * CFA_FEEDBACK_HEADERS.length)];
  const progressLines = _buildCumulativeProgressLines(user);

  const message =
    `${header}\n` +
    `──────────────\n` +
    `題目：${record.moduleName} - ${typeDisplay} ${record.number}\n` +
    `狀態：${statusDisplay}\n` +
    `累積練習題目（已看懂）：\n` +
    progressLines.join('\n');

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

  const volCode = `v${vol < 10 ? '0' + vol : vol}`;
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const rawRows = sheet.getRange(2, 1, sheet.getLastRow() - 1, 9).getValues();
  const col = user === 'Niu' ? 8 : 9;

  for (let i = 0; i < rawRows.length; i++) {
    const rowVol = String(rawRows[i][0] || '').trim().toLowerCase();
    const rowMod = String(rawRows[i][2] || '').trim().toLowerCase();
    if (rowVol === volCode && rowMod === modCode) {
      const rowIndex = i + 2;
      sheet.getRange(rowIndex, col).setValue(isLearned ? 'TRUE' : 'FALSE');
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
              label: '複習隨機題目（刷題模式）',
              text: '皮皮 CFA 題目 刷題模式',
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
  const volCode = `v${vol < 10 ? '0' + vol : vol}`;
  const modCode = `m${mod < 10 ? '0' + mod : mod}`;
  const allLearningModules = loadUserLearningModules();
  const lm = allLearningModules.find(m => m.volume.toLowerCase() === volCode && m.module.toLowerCase() === modCode);
  const moduleName = lm ? lm.moduleName : `LM${mod}`;

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
              text: `皮皮 CFA 學習狀態回報 V${vol} M${mod} && 皮皮 CFA 題目 V${vol} M${mod} 學習模式`,
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
 * Returns all learned modules that still have unlearned questions (learnedCount < totalCount).
 */
function getIncompleteLearnedModules(user: CfaUser, tabName: string = DEFAULT_CFA_TAB): UserLearningModule[] {
  const allLearningModules = loadUserLearningModules();
  const learnedModules = allLearningModules.filter(m => (user === 'Niu' ? m.isNiuLearned : m.isNuoLearned));
  const allQuestions = loadCfaQuestionsFromSheet(tabName);

  return learnedModules.filter(lm => {
    const modCode = lm.module.toLowerCase();
    const modQuestions = allQuestions.filter(q => q.module.toLowerCase() === modCode);
    const totalCount = modQuestions.length;
    if (totalCount === 0) return false;
    const learnedCount = modQuestions.filter(q => (user === 'Niu' ? q.niuLearned : q.nuoLearned) > 0).length;
    return learnedCount < totalCount;
  });
}

/**
 * Builds the Module Selector Flex Card for reviewing summaries, review quiz, or continuing learn quiz.
 */
function buildCfaModuleSelectorFlexCard(
  userId: string | undefined,
  mode: 'summary' | 'review_quiz' | 'learn_quiz'
): object {
  const user = resolveCfaUser(userId);
  const allLearningModules = loadUserLearningModules();
  const learnedModules = allLearningModules.filter(m => (user === 'Niu' ? m.isNiuLearned : m.isNuoLearned));

  let titleText = '📖 複習課本摘要';
  if (mode === 'review_quiz') {
    titleText = '✏️ 複習單元題目（錯題優先）';
  } else if (mode === 'learn_quiz') {
    titleText = '✏️ 繼續學習單元題目';
  }
  const altText = `📖 CFA LEVEL 1 · ${titleText}`;

  if (learnedModules.length === 0) {
    return {
      type: 'flex',
      altText,
      contents: {
        type: 'bubble',
        size: 'mega',
        header: {
          type: 'box',
          layout: 'vertical',
          backgroundColor: '#0D2538',
          paddingAll: 'lg',
          contents: [
            { type: 'text', text: '📖 CFA LEVEL 1', weight: 'bold', color: '#00C853', size: 'xs' },
            { type: 'text', text: titleText, weight: 'bold', color: '#FFFFFF', size: 'md', margin: 'xs', wrap: true }
          ]
        },
        body: {
          type: 'box',
          layout: 'vertical',
          paddingAll: 'lg',
          spacing: 'md',
          contents: [
            { type: 'text', text: `🐶 ${user} 目前尚未完成任何單元的導讀喔！請先開始第一單元！`, size: 'sm', color: '#333333', wrap: true },
            { type: 'separator', margin: 'md' },
            {
              type: 'button',
              style: 'primary',
              color: '#0D2538',
              height: 'sm',
              action: { type: 'message', label: '開始第一單元', text: '皮皮 CFA 課本摘要 V1 M1' }
            }
          ]
        }
      }
    };
  }

  let targetList = learnedModules;
  if (mode === 'learn_quiz') {
    targetList = getIncompleteLearnedModules(user);
    if (targetList.length === 0) {
      return {
        type: 'flex',
        altText,
        contents: {
          type: 'bubble',
          size: 'mega',
          header: {
            type: 'box',
            layout: 'vertical',
            backgroundColor: '#0D2538',
            paddingAll: 'lg',
            contents: [
              { type: 'text', text: '📖 CFA LEVEL 1', weight: 'bold', color: '#00C853', size: 'xs' },
              { type: 'text', text: titleText, weight: 'bold', color: '#FFFFFF', size: 'md', margin: 'xs', wrap: true }
            ]
          },
          body: {
            type: 'box',
            layout: 'vertical',
            paddingAll: 'lg',
            spacing: 'md',
            contents: [
              { type: 'text', text: `🎉 ${user} 目前所有已學單元的題目都已經看懂囉！可點選「複習單元題目」或「複習隨機題目」！`, size: 'sm', color: '#333333', wrap: true },
              { type: 'separator', margin: 'md' },
              {
                type: 'button',
                style: 'primary',
                color: '#0D2538',
                height: 'sm',
                action: { type: 'message', label: '複習單元題目', text: '皮皮 CFA 複習題目' }
              }
            ]
          }
        }
      };
    }
  }

  const buttons: object[] = targetList.map(lm => {
    const volNum = parseInt(lm.volume.replace(/\D/g, ''), 10) || 1;
    const modNum = parseInt(lm.module.replace(/\D/g, ''), 10) || 1;

    let cmdText = `皮皮 CFA 課本摘要 V${volNum} M${modNum}`;
    if (mode === 'review_quiz') {
      cmdText = `皮皮 CFA 題目 V${volNum} M${modNum} 複習模式`;
    } else if (mode === 'learn_quiz') {
      cmdText = `皮皮 CFA 題目 V${volNum} M${modNum} 學習模式`;
    }

    const rawLabel = `LM${modNum}: ${lm.moduleName}`;
    const label = rawLabel.length <= 38 ? rawLabel : rawLabel.slice(0, 37) + '…';

    return {
      type: 'button',
      style: 'primary',
      color: '#0D2538',
      height: 'sm',
      action: {
        type: 'message',
        label,
        text: cmdText
      }
    };
  });

  return {
    type: 'flex',
    altText,
    contents: {
      type: 'bubble',
      size: 'mega',
      header: {
        type: 'box',
        layout: 'vertical',
        backgroundColor: '#0D2538',
        paddingAll: 'lg',
        contents: [
          { type: 'text', text: '📖 CFA LEVEL 1', weight: 'bold', color: '#00C853', size: 'xs' },
          { type: 'text', text: titleText, weight: 'bold', color: '#FFFFFF', size: 'md', margin: 'xs', wrap: true }
        ]
      },
      body: {
        type: 'box',
        layout: 'vertical',
        paddingAll: 'lg',
        spacing: 'sm',
        contents: buttons,
      }
    }
  };
}

/**
 * Builds the single Flex Card for CFA Progress Summary.
 */
function _buildCfaProgressFlexCard(
  user: CfaUser,
  hasLearnedModules: boolean,
  hasIncompleteLearnedModules: boolean,
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
    if (hasIncompleteLearnedModules) {
      buttonContents.push({
        type: 'button',
        style: 'primary',
        color: '#0D2538',
        height: 'sm',
        action: {
          type: 'message',
          label: '繼續學習單元題目',
          text: '皮皮 CFA 學習題目',
        },
      });
    }

    buttonContents.push(
      {
        type: 'button',
        style: 'primary',
        color: '#0D2538',
        height: 'sm',
        action: {
          type: 'message',
          label: `下一單元（V${nextVol}／LM${nextMod}）`,
          text: `皮皮 CFA 課本摘要 V${nextVol} M${nextMod}`,
        },
      },
      {
        type: 'button',
        style: 'secondary',
        height: 'sm',
        action: {
          type: 'message',
          label: '複習單元摘要',
          text: '皮皮 CFA 複習摘要',
        },
      },
      {
        type: 'button',
        style: 'secondary',
        height: 'sm',
        action: {
          type: 'message',
          label: '複習單元題目',
          text: '皮皮 CFA 複習題目',
        },
      },
      {
        type: 'button',
        style: 'primary',
        color: '#00C853',
        height: 'sm',
        action: {
          type: 'message',
          label: '複習隨機題目（刷題模式）',
          text: '皮皮 CFA 題目 刷題模式',
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
  const incompleteModules = getIncompleteLearnedModules(user);
  const hasIncomplete = incompleteModules.length > 0;

  let nextVol = 1;
  let nextMod = 1;

  if (hasLearned) {
    const last = learnedModules[learnedModules.length - 1];
    nextVol = parseInt(last.volume.replace(/\D/g, ''), 10) || 1;
    const currentModNum = parseInt(last.module.replace(/\D/g, ''), 10) || 1;
    nextMod = currentModNum + 1;
  }

  const progressLines = _buildCumulativeProgressLines(user);
  return _buildCfaProgressFlexCard(user, hasLearned, hasIncomplete, nextVol, nextMod, progressLines);
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
