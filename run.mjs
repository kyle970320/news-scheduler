// news_ingest_and_score_with_circuit_breaker_all_insights_filtered.js
import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";
import { GoogleGenerativeAI } from "@google/generative-ai";

/** =========================
 *  0) ENV
 * ======================= */
const API_BASE = process.env.NEWS_API_BASE;
const API_KEY  = process.env.NEWS_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const DISCORD_WEBHOOK= process.env.DISCORD_WEBHOOK;

// Gemini
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY;
const GEMINI_MODEL_ID = process.env.GEMINI_MODEL_ID || "gemini-2.0-flash-exp";
const SCORE_BATCH_SIZE = Number(process.env.SCORE_BATCH_SIZE || 20);
const ENABLE_SCORING = process.env.ENABLE_SCORING !== "false";

// Upsert 기준 컬럼 (UNIQUE 인덱스 권장)
const UPSERT_ON = process.env.UPSERT_ON || "article_url";

// 신뢰도 가중치
const ALERT_STRONG_SCORE = Number(process.env.ALERT_STRONG_SCORE ?? 60);
const ALERT_STRONG_MODEL = Number(process.env.ALERT_STRONG_MODEL ?? 0.75);
const ALERT_STRONG_RULE  = Number(process.env.ALERT_STRONG_RULE  ?? 0.70);
const MAX_ALERT_ITEMS    = Number(process.env.MAX_ALERT_ITEMS ?? 8);


// 신뢰도 계산 헬퍼
function hasStrongBullish(row) {
    const insights = row?.sentiment_insights
    const filterdInsights = insights?.filter((el) => el.base_sentiment !=='neutral')
    if(filterdInsights?.length < 1){
        return false
    }
    const hasStrongInsight = filterdInsights?.some((el)=>{
        return (
          typeof el.score === "number" &&
          el.score >= ALERT_STRONG_SCORE &&
          (el.conf_model ?? 0) >= ALERT_STRONG_MODEL &&
          (el.conf_rule  ?? 0) >= ALERT_STRONG_RULE
        );
    })
    return hasStrongInsight ?? false
}

function hasStrongBearish(row) {
    const insights = row?.sentiment_insights
    const filterdInsights = insights?.filter((el) => el.base_sentiment !=='neutral')
    if(filterdInsights?.length < 1){
        return false
    }
    const hasStrongInsight = filterdInsights?.some((el)=>{
        return (
          typeof el.score === "number" &&
          el.score <= -ALERT_STRONG_SCORE &&
          (el.conf_model ?? 0) >= ALERT_STRONG_MODEL &&
          (el.conf_rule  ?? 0) >= ALERT_STRONG_RULE
        );
    })
    return hasStrongInsight ?? false
}

//알림 헬퍼
function buildAlertMessage(bulls, bears, upsertData, homepageUrl) {
  const header = `🚨 뉴스 알림 (호재/악재 컷오프 통과)\n`;
    // `• 기준: score≥${ALERT_STRONG_SCORE} |model≥${ALERT_STRONG_MODEL} |rule≥${ALERT_STRONG_RULE}\n`;

 const fmt = (r) => {
    const t = (r.title || "").slice(0, 120);
    const s = r.sentiment_score;
    const cm = (r.sentiment_confidence_model ?? 0).toFixed(2);
    const cr = (r.sentiment_confidence_rule ?? 0).toFixed(2);
    const tick =
      Array.isArray(r.tickers) && r.tickers.length
        ? ` [${r.tickers.slice(0, 4).join(",")}]`
        : "";

    // upsertData에서 같은 article_url을 가진 항목 찾기
    const matched = upsertData.find((u) => u.article_url === r.article_url);
    // 있으면 내부 링크, 없으면 원본 URL
    const link = matched
      ? `${homepageUrl.replace(/\/$/, "")}/${matched.id}`
      : r.article_url || "";

    // 디스코드는 <url> 형식으로 링크가 잘 열림
    return `• ${t}${tick}\n  score=${s}, cm=${cm}, cr=${cr}\n  <${link}>`;
  };

  const bullLines = bulls.slice(0, MAX_ALERT_ITEMS).map(fmt).join("\n\n");
  const bearLines = bears.slice(0, MAX_ALERT_ITEMS).map(fmt).join("\n\n");

  let body = "";
  if (bulls.length) body += `\n🟩 강한 호재 ${bulls.length}건\n${bullLines}\n`;
  if (bears.length) body += `\n🟥 강한 악재 ${bears.length}건\n${bearLines}\n`;

  return header + (body || "\n(해당 없음)");
}

const TABLE = "news";
const LIMIT = Number(process.env.NEWS_FETCH_LIMIT || 300);

if (!API_BASE || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("❌ Missing env (NEWS_API_BASE, SUPABASE_URL, SUPABASE_SERVICE_ROLE)");
  process.exit(1);
}
if (ENABLE_SCORING && !GOOGLE_API_KEY) {
  console.error("❌ ENABLE_SCORING=true 이지만 GOOGLE_API_KEY 가 없습니다.");
  process.exit(1);
}

// ===== helpers =====
const toISO = (d) => new Date(d).toISOString();

function lastHourWindow() {
  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  return { gteISO: toISO(oneHourAgo) };
}
/** =========================
 *  1) Supabase
 * ======================= */
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false }
});

/** =========================
 *  2) Cleanup (by created_at)
 * ======================= */
async function cleanupOldNews(days = 2) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffISO = cutoff.toISOString();
  console.log(`[CLEANUP] Deleting records older than ${cutoffISO}`);

  const { error } = await supabase
    .from(TABLE)
    .delete()
    .lt("created_at", cutoffISO);

  if (error) throw error;
}

/** =========================
 *  3) Discord 알림
 * ======================= */
async function sendDiscord(summaryText = "최신 뉴스가 갱신되었습니다!") {
  if (!DISCORD_WEBHOOK) {
    console.error("❌ Missing DISCORD_WEBHOOK");
    return;
  }

  // 디스코드 메시지는 content 최대 2000자 제한
  const MAX_LEN = 2000;
  const chunks = [];

  if (summaryText.length <= MAX_LEN) {
    chunks.push(summaryText);
  } else {
    // \n 단위로 잘라서 최대 2000자씩 분할
    let buffer = "";
    for (const line of summaryText.split("\n")) {
      if ((buffer + "\n" + line).length > MAX_LEN) {
        chunks.push(buffer);
        buffer = line;
      } else {
        buffer += (buffer ? "\n" : "") + line;
      }
    }
    if (buffer) chunks.push(buffer);
  }

  // 순차적으로 전송
  for (const [i, chunk] of chunks.entries()) {
    const payload = {
      username: "호외요 호외",
      content: chunk + (chunks.length > 1 ? `\n\n(${i + 1}/${chunks.length})` : "")
    };

    const res = await fetch(DISCORD_WEBHOOK, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });

    if (!res.ok) {
      const t = await res.text().catch(() => "");
      throw new Error(`Discord error: ${res.status} ${t}`);
    }
  }
}

/** =========================
 *  4) 외부 뉴스 API (항상 전 종목)
 * ======================= */
async function fetchNews({ gteISO, ticker = null }) {
  const url = new URL("/v2/reference/news", API_BASE);
  url.searchParams.set("sort", "published_utc");
  url.searchParams.set("order", "asc");
  url.searchParams.set("limit", LIMIT);
  url.searchParams.set("published_utc.gte", gteISO);
  if (ticker) url.searchParams.set("ticker", ticker);

  const headers = {};
  if (API_KEY) headers["Authorization"] = `Bearer ${API_KEY}`;

  // 1️⃣ 외부 뉴스 API 호출
  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`News API ${res.status}: ${body}`);
  }
  const json = await res.json();
  const items = json.results || [];
  if (!items.length) return [];

  // 2️⃣ DB에 이미 있는 기사 URL 목록 불러오기
  const urls = items
    .map((n) => n.article_url?.trim())
    .filter((u) => !!u);

  const { data: existing, error } = await supabase
    .from("news")
    .select("article_url")
    .in("article_url", urls);

  if (error) {
    console.error("[fetchNews] failed to check duplicates:", error);
    throw error;
  }

  const existingUrls = new Set(existing?.map((r) => r.article_url));

  // 3️⃣ 중복 제거
  const filtered = items.filter(
    (n) => !existingUrls.has(n.article_url?.trim())
  );

  console.log(
    `[fetchNews] total=${items.length} duplicates=${items.length - filtered.length} inserted=${filtered.length}`
  );

  return filtered;
}

/** =========================
 *  5) 소스/이벤트 분류 (간단 규칙)
 * ======================= */
function getDomain(url) {
  try { return url ? new URL(url).hostname.toLowerCase() : ""; }
  catch { return ""; }
}
function sourceKindFromUrl(articleUrl) {
  const h = getDomain(articleUrl);
  if (!h) return "unknown";

  // Wire-type press releases
  if (/(prnewswire|businesswire|globenewswire|accesswire|newsfile|nasdaq\.com)/.test(h)) {
    return "wire";
  }

  // Major global business media
  if (/(reuters|bloomberg|wsj\.com|ft\.com|apnews\.com|cnbc\.com|marketwatch\.com|forbes\.com|businessinsider\.com|barrons\.com|financialtimes\.com|seekingalpha\.com)/.test(h)) {
    return "major_press";
  }

  // Regional / national press
  if (/(nikkei\.com|theguardian\.com|bbc\.com|cnn\.com|abcnews\.go\.com|nbcnews\.com|telegraph\.co\.uk|economictimes\.indiatimes\.com|thehindu\.com|scmp\.com|afr\.com|straitstimes\.com)/.test(h)) {
    return "regional_press";
  }

  // Financial portals / investor media
  if (/(finance\.yahoo\.com|benzinga\.com|tipranks\.com|thestreet\.com|fxstreet\.com|investing\.com|marketscreener\.com|zacks\.com)/.test(h)) {
    return "financial_portal";
  }

  // Corporate IR / company pages
  if (/(\.ir\.|\.corp\.|investors\.|about\.|press\.|media\.)/.test(h)) {
    return "company_ir";
  }

  // Blogs, forums, community
  if (/(medium\.com|substack\.com|wordpress\.com|blogspot\.com|reddit\.com|stocktwits\.com|motleyfool\.com|discord\.gg)/.test(h)) {
    return "blog_or_forum";
  }

  return "unknown";
}
function eventKindFromKeywordsOrText(keywords, text) {
  const t = `${(keywords || []).join(" ").toLowerCase()} ${String(text || "").toLowerCase()}`;
  if (/\b(m&a|acquisition|merger|buyout|takeover)\b/.test(t)) return "ma";
  if (/\b(fda|pdufa|phase\s*(1|2|3)|trial|ind|approval|clearance)\b/.test(t)) return "fda";
  if (/\b(class action|securities lawsuit|lawsuit|litigation)\b/.test(t)) return "lawsuit";
  if (/\b(earnings|q\d|eps|revenue|results)\b/.test(t)) return "earnings";
  if (/\b(guidance|outlook|raise guidance|lower guidance)\b/.test(t)) return "guidance";
  if (/\b(partnership|collaboration|contract|deal)\b/.test(t)) return "partnership";
  if (/\b(regulator|regulatory|sec|doj|antitrust|cfius|fine|penalty)\b/.test(t)) return "regulatory";
  return "other";
}

/** =========================
 *  6) 테이블 스키마 매핑 (+ insight 보존)
 * ======================= */
function mapToRows(items) {
  return items.map((r) => {
    const title = r.title ?? null;
    const description = r.description ?? r.summary ?? null;
    const article_url = r.article_url ?? null;
    const published = r.published_utc ?? r.published_at ?? r.date ?? null;
    const tickers = Array.isArray(r.tickers) ? r.tickers : [];
    const insights = Array.isArray(r?.insights) ? r.insights : []; // ← 스키마 가정: [{ticker,sentiment,sentiment_reasoning}]
    const keywords = Array.isArray(r.keywords) ? r.keywords : [];

    return {
      title,
      description,
      article_url,
      keywords,
      published_utc: published ? new Date(published).toISOString() : null,
      insights,     // 원본 보존
      tickers,
      // 기사 롤업 필드
      sentiment_score: null,
      sentiment_confidence_model: null,
      sentiment_confidence_rule: null,
      sentiment_reasoning: null,
      // insight 레벨 결과
      sentiment_insights: null, // [{index, ticker, base_sentiment, text, score, conf_model, conf_rule, reasoning}]
    };
  });
}

/** =========================
 *  7) Gemini — insight 단위 프롬프트
 * ======================= */
const genAI = ENABLE_SCORING && GOOGLE_API_KEY ? new GoogleGenerativeAI(GOOGLE_API_KEY) : null;

// 입력 스키마에 맞춰 insight 텍스트 구성
function toInsightTextFromSchema(ins) {
  // 안전 방어
  const ticker = ins?.ticker ?? "";
  const sentiment = ins?.sentiment ?? "";
  const reason = ins?.sentiment_reasoning ?? "";
  // 모델에 전달할 요약 텍스트
  return `Sentiment=${sentiment}; Ticker=${ticker}; Rationale=${reason}`;
}

function buildPromptForInsights(batch) {
  const header = `
You are a financial sentiment analyzer.
Re-evaluate EACH INSIGHT (already labeled positive/negative) and assign a sentiment score from -100 to +100 (0=neutral) focused on market impact.
Return ONLY a valid JSON array. No extra text.

Rules:
- "sentiment_score": integer [-100, 100]
- "confidence": float [0, 1] with two decimals
- "reasoning_summary": <= 25 words, in Korean, concise and specific
- Preserve input order via "index"
- If info is insufficient, use score 0 and confidence <= 0.40

Output JSON schema:
[
  { "index": <number>, "sentiment_score": <int>, "confidence": <float>, "reasoning_summary": "<string>" },
  ...
]
`.trim();

  const body = batch.map((u, i) => [
    `--- INSIGHT ${i} ---`,
    `ArticleTitle: ${u.articleTitle ?? ""}`,
    `Insight: ${u.text ?? ""}`,
    `Published UTC: ${u.published_utc ?? ""}`
  ].join("\n")).join("\n\n");

  return `${header}\n\n${body}\n\nReturn the JSON array now.`;
}

async function scoreBatchGeminiInsights(batch, attempt = 0) {
  if (!genAI) throw new Error("Gemini not initialized");
  const model = genAI.getGenerativeModel({
    model: GEMINI_MODEL_ID,
    generationConfig: { temperature: 0.1, responseMimeType: "application/json" },
  });
  const prompt = buildPromptForInsights(batch);
  try {
    const result = await model.generateContent(prompt);
    const text = result.response.text();
    const arr = safeExtractJsonArray(text);
    return arr.map((r, idx) => ({
      index: Number.isFinite(r?.index) ? r.index : idx,
      sentiment_score: Math.max(-100, Math.min(100, Math.trunc(r?.sentiment_score ?? 0))),
      confidence: Math.max(0, Math.min(1, Number(r?.confidence ?? 0))),
      reasoning_summary: String(r?.reasoning_summary ?? "").slice(0, 300),
    }));
  } catch (e) {
    const msg = String(e?.message ?? e);

    if (/(rate|quota|exceed|429|insufficient|limit)/i.test(msg)) {
      await disableScoringForToday(msg);
      return batch.map((_, i) => ({
        index: i, sentiment_score: 0, confidence: 0.3,
        reasoning_summary: "Scoring disabled due to quota/limit."
      }));
    }
    if (/5\d\d|temporar|unavailable/i.test(msg) && attempt < 4) {
      const backoff = Math.min(15000, 2000 * 2 ** attempt);
      console.warn(`[Gemini] retry in ${backoff}ms (attempt ${attempt+1}) :: ${msg}`);
      await sleep(backoff);
      return scoreBatchGeminiInsights(batch, attempt + 1);
    }
    console.error(`[Gemini] failed: ${msg}`);
    return batch.map((_, i) => ({
      index: i, sentiment_score: 0, confidence: 0.3,
      reasoning_summary: "Model error; defaulted to neutral."
    }));
  }
}

function safeExtractJsonArray(text) {
  const m = text.match(/\[([\s\S]*)\]/);
  const candidate = m ? `[${m[1]}]` : text;
  const parsed = JSON.parse(candidate);
  if (!Array.isArray(parsed)) throw new Error("Model did not return array.");
  return parsed;
}

/** =========================
 *  8) 보정/롤업
 * ======================= */
function clamp01(x) { return Math.max(0, Math.min(1, x)); }
function sigmoid(z) { return 1 / (1 + Math.exp(-z)); }
function logit(p) { return Math.log(p / (1 - p)); }

function sourceTrust(source) {
  switch (source) {
    case "wire":             return 0.80;
    case "major_press":      return 0.75;
    case "regional_press":   return 0.68;
    case "financial_portal": return 0.62;
    case "company_ir":       return 0.55;
    case "blog_or_forum":    return 0.45;
    default:                 return 0.50;
  }
}
function eventWeight(event) {
  switch (event) {
    case "ma":
    case "fda":
    case "regulatory":
    case "lawsuit":     return 0.75;
    case "earnings":
    case "guidance":    return 0.68;
    case "partnership": return 0.62;
    default:            return 0.55;
  }
}
function pseudoCalibrate(input, opts = {}) {
  const {
    k = 4, s0 = 0.5,
    wIntensity = 1.0, wModel = 1.0, wSource = 0.7, wEvent = 0.8,
  } = opts;

  const score = Math.max(-100, Math.min(100, Math.trunc(input.sentiment_score)));
  const mag = Math.abs(score) / 100;
  const p_intensity = clamp01(sigmoid(k * (mag - s0)));
  const p_model = clamp01(typeof input.confidence_model === "number" ? input.confidence_model : 0.5);
  const p_source = clamp01(sourceTrust(input.source));
  const p_event  = clamp01(eventWeight(input.event));

  const eps = 1e-6;
  const parts = [
    [p_intensity, wIntensity],
    [p_model,     wModel],
    [p_source,    wSource],
    [p_event,     wEvent],
  ];
  let num = 0, den = 0;
  for (const [p, w] of parts) {
    const pp = Math.min(1 - eps, Math.max(eps, p));
    num += w * logit(pp);
    den += w;
  }
  const confidence_rule = clamp01(sigmoid(num / Math.max(den, 1e-6)));
  const score_pseudo = Math.sign(score) * Math.round(Math.abs(score) * (0.5 + confidence_rule / 2));
  return { confidence_rule, score_pseudo };
}

function rollupArticleSentimentFromInsights(row) {
  const srcKind = sourceKindFromUrl(row.article_url);
  const evtKind = eventKindFromKeywordsOrText(row.keywords || [], `${row.title ?? ""} ${row.description ?? ""}`);

  if (!Array.isArray(row.sentiment_insights) || row.sentiment_insights.length === 0) {
    return null;
  }

  // 모델 컨피던스 기반 가중 평균
  let num = 0, den = 0;
  for (const si of row.sentiment_insights) {
    const w = clamp01(Number(si.conf_model ?? 0.5));
    num += w * si.score;
    den += w;
  }
  const avgScore = den > 0 ? Math.round(num / den) : 0;

  const { confidence_rule } = pseudoCalibrate({
    sentiment_score: avgScore,
    confidence_model: clamp01(row.sentiment_insights.reduce((a,b)=>a + (b.conf_model ?? 0.5), 0) / Math.max(1,row.sentiment_insights.length)),
    source: srcKind,
    event: evtKind,
  });

  const rolled = Math.sign(avgScore) * Math.round(Math.abs(avgScore) * (0.5 + confidence_rule / 2));
  return {
    score: rolled,
    conf_model: clamp01(row.sentiment_insights.reduce((a,b)=>a + (b.conf_model ?? 0.5), 0) / Math.max(1,row.sentiment_insights.length)),
    conf_rule: confidence_rule,
    reasoning: "Insight-level rollup.",
  };
}

/** =========================
 *  9) DB Upsert (chunk)
 * ======================= */
async function upsertChunked(rows, chunkSize = 100) {
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
const { data, error } = await supabase
  .from(TABLE)
  .upsert(chunk, { onConflict: UPSERT_ON, ignoreDuplicates: true })
  .select();
    if (error){ 
        throw error
    };
    return {upsertData : data }
  }
}

/** =========================
 * 10) Insight 단위 확장/스코어/회수
 * ======================= */
// neutral은 모델 X, positive/negative만 모델 태움
function expandInsightUnits(rows) {
  const units = []; // [{ rowIdx, insightIdx, text, articleTitle, published_utc }]
  rows.forEach((row, rIdx) => {
    const arr = Array.isArray(row.insights) ? row.insights : [];
    if (!arr.length) return;

    // 먼저 neutral들을 즉시 결과에 0점으로 기록(모델 미호출)
    arr.forEach((ins, iIdx) => {
      if (!ins) return;
      const baseSent = String(ins.sentiment || "").toLowerCase();
      if (baseSent === "neutral") {
        row.sentiment_insights ||= [];
        row.sentiment_insights[iIdx] = {
          index: iIdx,
          ticker: ins.ticker ?? null,
          base_sentiment: baseSent,
          text: toInsightTextFromSchema(ins),
          score: 0,
          conf_model: 0.0,
          conf_rule: 0.0,
          reasoning: "Neutral from source; model skipped."
        };
      }
    });

    // positive/negative만 큐에 올림
    arr.forEach((ins, iIdx) => {
      if (!ins) return;
      const baseSent = String(ins.sentiment || "").toLowerCase();
      if (baseSent === "positive" || baseSent === "negative") {
        units.push({
          rowIdx: rIdx,
          insightIdx: iIdx,
          text: toInsightTextFromSchema(ins),
          articleTitle: row.title ?? "",
          published_utc: row.published_utc ?? null,
          base_sentiment: baseSent,
          ticker: ins.ticker ?? (Array.isArray(row.tickers) && row.tickers[0] ? row.tickers[0] : null),
        });
      }
    });
  });
  return units;
}

async function scoreInsightsForRows(rows) {
  const units = expandInsightUnits(rows);
  if (!units.length) return;

  let disabled = ENABLE_SCORING ? await isScoringDisabled() : true;
  if (disabled) {
    console.warn("[SCORE] disabled (flag) — positive/negative insights neutral fallback.");
    // 남은(모델 대상)들도 전부 뉴트럴로 채움
    units.forEach((u) => {
      const row = rows[u.rowIdx];
      row.sentiment_insights ||= [];
      row.sentiment_insights[u.insightIdx] = {
        index: u.insightIdx,
        ticker: u.ticker ?? null,
        base_sentiment: u.base_sentiment,
        text: u.text,
        score: 0, conf_model: 0.3, conf_rule: 0.3,
        reasoning: "Scoring disabled."
      };
    });
    return;
  }

  // 배치 스코어링
  for (let i = 0; i < units.length; i += SCORE_BATCH_SIZE) {
    disabled = await isScoringDisabled();
    if (disabled) {
      console.warn("[SCORE] disabled mid-run — remaining insights neutral.");
      for (let k = i; k < units.length; k++) {
        const u = units[k];
        const row = rows[u.rowIdx];
        row.sentiment_insights ||= [];
        row.sentiment_insights[u.insightIdx] = {
          index: u.insightIdx,
          ticker: u.ticker ?? null,
          base_sentiment: u.base_sentiment,
          text: u.text,
          score: 0, conf_model: 0.3, conf_rule: 0.3,
          reasoning: "Scoring disabled due to quota/limit."
        };
      }
      break;
    }

    const slice = units.slice(i, i + SCORE_BATCH_SIZE);
    const scored = await scoreBatchGeminiInsights(slice);

    // 결과 반영 + 룰 보정
    scored.forEach((s, j) => {
      const u = slice[j];
      const row = rows[u.rowIdx];
      const srcKind = sourceKindFromUrl(row.article_url);
      const evtKind = eventKindFromKeywordsOrText(row.keywords || [], `${row.title ?? ""} ${row.description ?? ""}`);

      const { confidence_rule } = pseudoCalibrate({
        sentiment_score: s.sentiment_score,
        confidence_model: s.confidence,
        source: srcKind,
        event: evtKind,
      });

      row.sentiment_insights ||= [];
      row.sentiment_insights[u.insightIdx] = {
        index: u.insightIdx,
        ticker: u.ticker ?? null,
        base_sentiment: u.base_sentiment, // 원본 레이블 보존
        text: u.text,
        score: s.sentiment_score,
        conf_model: s.confidence,
        conf_rule: confidence_rule,
        reasoning: s.reasoning_summary,
      };
    });

    console.log(`[SCORE] Insight rows ${i} ~ ${Math.min(i + SCORE_BATCH_SIZE - 1, units.length - 1)}`);
  }
}

/** =========================
 * 11) 실행 (전 종목 + insight 단위, neutral 제외)
 * ======================= */
async function runAll() {
  console.log(`\n[RUN] scope=ALL limit=${LIMIT}`);
  const { gteISO } = lastHourWindow();
  const items = await fetchNews({ gteISO });
  if (!items?.length) {
    console.log(`[RUN] No items.`);
    return { inserted: 0, count: 0 };
  }

  const rows = mapToRows(items);

  if (ENABLE_SCORING) {
    await scoreInsightsForRows(rows);

    // insight 기반 기사 롤업
    rows.forEach((row) => {
      const roll = rollupArticleSentimentFromInsights(row);
      if (roll) {
        row.sentiment_score = roll.score;
        row.sentiment_confidence_model = roll.conf_model;
        row.sentiment_confidence_rule = roll.conf_rule;
        row.sentiment_reasoning = roll.reasoning;
      } else {
        if (row.sentiment_score === null) {
          row.sentiment_score = 0;
          row.sentiment_confidence_model = 0.0;
          row.sentiment_confidence_rule = 0.0;
          row.sentiment_reasoning = "No insights to score (neutral).";
        }
      }
    });
  } else {
    // 전체 스코어링 비활성
    rows.forEach((row) => {
      const arr = Array.isArray(row.insights) ? row.insights : [];
      row.sentiment_insights = arr.map((ins, idx) => ({
        index: idx,
        ticker: ins?.ticker ?? null,
        base_sentiment: ins?.sentiment ?? null,
        text: toInsightTextFromSchema(ins),
        score: 0, conf_model: 0.0, conf_rule: 0.0,
        reasoning: "Scoring skipped."
      }));
      row.sentiment_score = 0;
      row.sentiment_confidence_model = 0.0;
      row.sentiment_confidence_rule = 0.0;
      row.sentiment_reasoning = "Scoring skipped.";
    });
  }

  const {upsertData} = await upsertChunked(rows);
  console.log(`[OK] upserted=${rows.length} scope=ALL`);
  return { rows, upsertData, inserted: rows.length, count: items.length };
}

/** =========================
 * 12) 회로차단/유틸
 * ======================= */
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function getNextPstMidnight() {
  const now = new Date();
  const next = new Date();
  next.setUTCHours(8, 0, 0, 0);
  if (now >= next) next.setUTCDate(next.getUTCDate() + 1);
  return next;
}
async function isScoringDisabled() {
  const { data, error } = await supabase
    .from("google_flag")
    .select("value")
    .eq("key", "scoring")
    .single();
  if (error && error.code !== "PGRST116") {
    console.warn("[FLAGS] read error", error.message);
  }
  const disabledUntilISO = data?.value?.disabled_until;
  if (!disabledUntilISO) return false;
  return new Date() < new Date(disabledUntilISO);
}
async function disableScoringForToday(reason = "quota/rate limit") {
  const untilISO = getNextPstMidnight().toISOString();
  const payload = { disabled_until: untilISO, reason };
  const { error } = await supabase
    .from("google_flag")
    .upsert({ key: "scoring", value: payload, updated_at: new Date().toISOString() });
  if (error) console.warn("[FLAGS] upsert error", error.message);
  console.warn(`[SCORE] disabled until PST midnight (${untilISO}) :: ${reason}`);
}

/** =========================
 * 13) 엔트리포인트
 * ======================= */
async function main() {
  await cleanupOldNews(2);
  const { rows, upsertData, inserted, count } = await runAll();

  const strongBulls = rows?.filter(hasStrongBullish) ?? [];
  const strongBears = rows?.filter(hasStrongBearish) ?? [];

  if (strongBulls.length || strongBears.length) {
    const msg = buildAlertMessage(strongBulls, strongBears, upsertData,'https://news-dashboard-fawn-nu.vercel.app');
    await sendDiscord(msg);
  }

  console.log(`[DONE] fetched=${count}, upserted=${inserted}, scope=ALL`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
