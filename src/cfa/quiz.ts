/**
 * CFA Level 1 Interactive Quiz Module for LINE Bot
 * Fetches dynamic question cards, solutions, evaluates answers,
 * and tracks individual learning progress (Nuo and Niu) via Google Sheets.
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

type CfaUser = 'Nuo' | 'Niu';

const DEFAULT_CFA_SPREADSHEET_ID = '1uHIp7LFpbol8V1qFlptvX0HYQuSp5bLgjYAZztmONHY';
const DEFAULT_CFA_TAB = 'v01-quantitative-methods';

/**
 * Opens the CFA Question Bank spreadsheet.
 */
function _getCfaSpreadsheet(): GoogleAppsScript.Spreadsheet.Spreadsheet {
  const id = getScriptProperty(PROP_KEYS.CFA_QUESTION_SPREADSHEET_ID) || DEFAULT_CFA_SPREADSHEET_ID;
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
 * 1. Questions with 0 answers for this user.
 * 2. If all answered, questions with the lowest accuracy ratio (Correct / Answered).
 */
function fetchNextCfaQuestion(
  userId?: string,
  tabName: string = DEFAULT_CFA_TAB
): { record: CfaQuestionRecord; flexMessage: object } | null {
  const records = loadCfaQuestionsFromSheet(tabName);
  if (records.length === 0) return null;

  const user = resolveCfaUser(userId);

  // 1. Check for un-answered questions
  const unAnswered = records.filter(r => {
    const ansCount = user === 'Niu' ? r.niuAnswered : r.nuoAnswered;
    return ansCount === 0;
  });

  let selected: CfaQuestionRecord;
  if (unAnswered.length > 0) {
    selected = unAnswered[0];
  } else {
    // 2. Sort by accuracy ratio ascending (lowest correct ratio first)
    const sorted = [...records].sort((a, b) => {
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
): { success: boolean; user: CfaUser; message: string } {
  const record = findCfaQuestionByRef(vol, mod, typeCode, num, tabName);
  if (!record) {
    return { success: false, user: 'Nuo', message: `找不到題目 V${vol} M${mod} ${typeCode}${num}` };
  }

  const user = resolveCfaUser(userId);
  const sheet = _getCfaSpreadsheet().getSheetByName(tabName);
  if (!sheet) {
    return { success: false, user, message: `找不到試算表分頁 ${tabName}` };
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

  const message =
    `${header}\n` +
    `──────────────\n` +
    `• 題目：${record.moduleName} - ${typeDisplay} ${record.number}\n` +
    `• 狀態：${statusDisplay}\n` +
    `• 累計練習：${newAns} 次（答對 ${newCor} 次，正確率 ${Math.round((newCor / newAns) * 100)}%）`;

  return { success: true, user, message };
}
