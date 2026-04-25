function callOpenAI(systemPrompt: string, userMessage: string): string {
  const apiKey = getScriptProperty(PROP_KEYS.OPENAI_API_KEY);

  const payload = {
    model: 'gpt-4o-mini',
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userMessage },
    ],
    temperature: 0.7,
    max_tokens: 2048,
  };

  const response = UrlFetchApp.fetch(OPENAI_API_BASE, {
    method: 'post' as GoogleAppsScript.URL_Fetch.HttpMethod,
    contentType: 'application/json',
    headers: { Authorization: `Bearer ${apiKey}` },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true,
  });

  if (response.getResponseCode() !== 200) {
    throw new Error(`OpenAI API error ${response.getResponseCode()}: ${response.getContentText()}`);
  }

  const json = JSON.parse(response.getContentText());
  const text = json.choices?.[0]?.message?.content;
  if (!text) throw new Error('Empty response from OpenAI');
  return text;
}

function buildNewsAnalysisPrompt(ticker: string, newsItems: NewsItem[]): { system: string; user: string } {
  const newsContext = newsItems.map((n, i) => {
    const date = formatDateTW(n.date);
    return `${i + 1}. [${date}] ${n.title}\n   摘要: ${n.snippet}\n   來源: ${n.url}`;
  }).join('\n\n');

  const system = `你是一位專業的股權研究分析師 (Professional Equity Research Analyst)。
請使用繁體中文（台灣用語）進行分析。

指令規範：
1. 綜合新聞內容，解釋指定個股近期的股價波動原因或潛在趨勢。
2. 使用學術寫作風格 (Academic Writing Style)，論述須具備邏輯性與分析深度。
3. 明確引用來源（例如：「根據 CNBC (2026/04/20) 報導...」）。
4. 輸出格式：
   - 先寫分析段落（2-3 段）
   - 最後列出「參考資料 (References)」區塊，格式為：[新聞標題](URL)

請直接輸出分析內容，不要加額外的前言。`;

  const user = `請分析 ${ticker} 的近期股價波動原因或趨勢。以下是 7 天內的新聞資料：

${newsContext}`;

  return { system, user };
}

function analyzeStockWithNews(ticker: string, newsItems: NewsItem[]): string {
  const { system, user } = buildNewsAnalysisPrompt(ticker, newsItems);
  return retryWithBackoff(() => callOpenAI(system, user), 2, 5000);
}

const CHAT_SYSTEM_PROMPT = `你是一位專業的美股投資助理，請使用繁體中文（台灣用語）回答。
保持回答簡潔、專業且有幫助。如果被問到非投資相關問題，禮貌地引導回美股話題。`;

function handleChatQuery(userMessage: string): string {
  return retryWithBackoff(() => callOpenAI(CHAT_SYSTEM_PROMPT, userMessage), 2, 5000);
}
