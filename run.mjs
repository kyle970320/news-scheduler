// news_ingest_and_score.js
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
const GOOGLE_API_KEY = process.env.GOOGLE_API_KEY; // 반드시 설정
const GEMINI_MODEL_ID = process.env.GEMINI_MODEL_ID || "gemini-2.0-flash-exp";
const SCORE_BATCH_SIZE = Number(process.env.SCORE_BATCH_SIZE || 20); // 20 권장
const ENABLE_SCORING = process.env.ENABLE_SCORING !== "false"; // 필요 시 끄기

if (!API_BASE || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  console.error("❌ Missing env (NEWS_API_BASE, SUPABASE_URL, SUPABASE_SERVICE_ROLE)");
  process.exit(1);
}
if (ENABLE_SCORING && !GOOGLE_API_KEY) {
  console.error("❌ ENABLE_SCORING=true 이지만 GOOGLE_API_KEY 가 없습니다.");
  process.exit(1);
}

const TABLE = "news";
const LIMIT = 300;

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
async function sendDiscord() {
  if (!DISCORD_WEBHOOK) {
    console.error("❌ Missing DISCORD_WEBHOOK");
    return;
  }
  const payload = {
    username: "호외요 호외",
    content: "최신 뉴스가 갱신되었습니다!"
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

/** =========================
 *  4) Helpers (시간/윈도우)
 * ======================= */
const toISO = (d) => new Date(d).toISOString();
function lastHourWindow() {
  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  return { gteISO: toISO(oneHourAgo) };
}

/** =========================
 *  5) 외부 뉴스 API
 * ======================= */
async function fetchNews({ gteISO, ticker = null }) {
  const url = new URL("/v2/reference/news", API_BASE);
  url.searchParams.set("sort", "published_utc");
  url.searchParams.set("order", "asc");
  url.searchParams.set("limit", String(LIMIT));
  url.searchParams.set("published_utc.gte", gteISO);
  if (ticker) url.searchParams.set("ticker", ticker);
  const headers = {};
  if (API_KEY) headers["Authorization"] = `Bearer ${API_KEY}`;

  const res = await fetch(url.toString(), { headers });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`News API ${res.status}: ${body}`);
  }
  const json = await res.json();
  return json.results || [];
}

/** =========================
 *  6) 소스/이벤트 분류 (간단 규칙)
 * ======================= */
function getDomain(url) {
  try { return url ? new URL(url).hostname.toLowerCase() : ""; }
  catch { return ""; }
}
function sourceKindFromUrl(articleUrl) {
  const h = getDomain(articleUrl);
  if (!h) return "unknown";
  if (/(prnewswire|businesswire|globenewswire|nasdaq\.com|newsfile|accesswire)/.test(h)) return "wire";
  if (/(reuters|bloomberg|wsj|ft\.com|apnews|cnbc|marketwatch|forbes)/.test(h)) return "major_press";
  if (/(medium\.com|substack|wordpress|blogspot)/.test(h)) return "blog";
  if (/\.corp\.|\.ir\./.test(h)) return "company";
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
 *  7) 테이블 스키마 매핑 (+ 감정 필드)
 * ======================= */
function mapToRows(items) {
  return items.map((r) => {
    const title = r.title ?? null;
    const description = r.description ?? r.summary ?? null;
    const article_url = r.article_url ?? null;
    const published = r.published_utc ?? r.published_at ?? r.date ?? null;
    const tickers = Array.isArray(r.tickers) ? r.tickers : [];
    const insights = r?.insights ?? {};
    const keywords = Array.isArray(r.keywords) ? r.keywords : [];

    return {
      title,
      description,
      article_url,
      keywords,
      published_utc: published ? new Date(published).toISOString() : null,
      insights,
      tickers,
      // scoring fields
      sentiment_score: null,
      sentiment_confidence_model: null,
      sentiment_confidence_rule: null,
      sentiment_reasoning: null,
    };
  });
}

/** =========================
 *  8) Gemini 배치 스코어링
 * ======================= */
const genAI = GOOGLE_API_KEY ? new GoogleGenerativeAI(GOOGLE_API_KEY) : null;

function buildPrompt(batch) {
  const header = `
You are a financial sentiment analyzer.
For EACH article, assign a sentiment score from -100 (extremely negative) to +100 (extremely positive), 0 is neutral.
Consider tone, language intensity, and potential market impact (lawsuits, FDA/M&A, earnings, guidance, partnerships, layoffs, accounting issues).
Return ONLY a valid JSON array. No extra text.

Rules:
- "sentiment_score": integer in [-100, 100]
- "confidence": float in [0, 1] with two decimals
- "reasoning_summary": <= 25 words; concise and specific
- Preserve input order via "index"
- If info is insufficient, use score 0 and confidence <= 0.40

Output JSON schema:
[
  { "index": <number>, "ticker": "<string|null>", "sentiment_score": <int>, "confidence": <float>, "reasoning_summary": "<string>" },
  ...
]
`.trim();

  const body = batch.map((a, i) => [
    `--- ARTICLE ${i} ---`,
    `Title: ${a.title ?? ""}`,
    `Description: ${a.description ?? ""}`,
    `Ticker: ${a.ticker ?? ""}`,
    `Published UTC: ${a.published_utc ?? ""}`
  ].join("\n")).join("\n\n");

  return `${header}\n\n${body}\n\nReturn the JSON array now.`;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function scoreBatchGemini(batch, attempt = 0) {
  if (!genAI) throw new Error("Gemini not initialized");
  const model = genAI.getGenerativeModel({
    model: GEMINI_MODEL_ID,
    generationConfig: {
      temperature: 0.1,
      responseMimeType: "application/json",
    },
  });
  const prompt = buildPrompt(batch);
  try {
    const result = await model.generateContent(prompt);
    const text = result.response.text();
    const arr = safeExtractJsonArray(text);
    return arr.map((r, idx) => ({
      index: Number.isFinite(r?.index) ? r.index : idx,
      ticker: r?.ticker ?? (batch[idx]?.ticker ?? null),
      sentiment_score: Math.max(-100, Math.min(100, Math.trunc(r?.sentiment_score ?? 0))),
      confidence: Math.max(0, Math.min(1, Number(r?.confidence ?? 0))),
      reasoning_summary: String(r?.reasoning_summary ?? "").slice(0, 300),
    }));
  } catch (e) {
    const msg = String(e?.message ?? e);
    if (/(429|quota|rate|temporar|unavailable|5\d\d)/i.test(msg) && attempt < 4) {
      const backoff = Math.min(15000, 2000 * 2 ** attempt);
      console.warn(`[Gemini] retry in ${backoff}ms (attempt ${attempt+1}) :: ${msg}`);
      await sleep(backoff);
      return scoreBatchGemini(batch, attempt + 1);
    }
    console.error(`[Gemini] failed: ${msg}`);
    // 실패 시 뉴트럴 fallback
    return batch.map((_, i) => ({
      index: i,
      ticker: batch[i].ticker ?? null,
      sentiment_score: 0,
      confidence: 0.3,
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
 *  9) 간이 보정 (pseudo-calibration)
 * ======================= */
function clamp01(x) { return Math.max(0, Math.min(1, x)); }
function sigmoid(z) { return 1 / (1 + Math.exp(-z)); }
function logit(p) { return Math.log(p / (1 - p)); }

function sourceTrust(source) {
  switch (source) {
    case "wire":        return 0.80;
    case "major_press": return 0.75;
    case "company":     return 0.55;
    case "blog":        return 0.50;
    default:            return 0.50; // unknown
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
    wIntensity = 1.0, wModel = 1.0, wSource = 0.7, wEvent = 0.8, wPrice = 0.5,
    r0 = 0.02,
  } = opts;

  const score = Math.max(-100, Math.min(100, Math.trunc(input.sentiment_score)));
  const mag = Math.abs(score) / 100;
  const p_intensity = clamp01(sigmoid(k * (mag - s0)));
  const p_model = clamp01(typeof input.confidence_model === "number" ? input.confidence_model : 0.5);
  const p_source = clamp01(sourceTrust(input.source));
  const p_event  = clamp01(eventWeight(input.event));

  let p_price;
  if (typeof input.short_return_abs === "number") {
    const r = Math.abs(input.short_return_abs);
    p_price = clamp01(0.5 + 0.5 * Math.tanh(r / r0));
  }

  const eps = 1e-6;
  const parts = [
    [p_intensity, wIntensity],
    [p_model,     wModel],
    [p_source,    wSource],
    [p_event,     wEvent],
  ];
  if (p_price !== undefined) parts.push([p_price, wPrice]);

  let num = 0, den = 0;
  for (const [p, w] of parts) {
    const pp = Math.min(1 - eps, Math.max(eps, p));
    num += w * logit(pp);
    den += w;
  }
  const confidence_rule = clamp01(sigmoid(num / Math.max(den, 1e-6)));

  const score_pseudo = Math.sign(score) * Math.round(
    Math.abs(score) * (0.5 + confidence_rule / 2)
  );

  return { confidence_rule, score_pseudo, components: { p_intensity, p_model, p_source, p_event, ...(p_price !== undefined ? { p_price } : {}) } };
}

/** =========================
 * 10) DB Insert (chunk)
 * ======================= */
async function insertChunked(rows, chunkSize = 100) {
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const { error } = await supabase.from(TABLE).insert(chunk);
    if (error) throw error;
  }
}

/** =========================
 * 11) 전체 파이프라인
 * ======================= */
async function main() {
  const { gteISO } = lastHourWindow();

  // 오래된 뉴스 삭제(생성일 기준)
  await cleanupOldNews(2);

  // 뉴스 수집
  const items = await fetchNews({ gteISO });
  if (!items?.length) {
    console.log(`[OK] No items. window=${gteISO}`);
    return;
  }

  // 기본 스키마 매핑
  const rows = mapToRows(items);

  // 감정 스코어링 (배치)
  if (ENABLE_SCORING) {
    console.log(`[SCORE] Gemini scoring enabled. model=${GEMINI_MODEL_ID}, batch=${SCORE_BATCH_SIZE}`);
    for (let i = 0; i < rows.length; i += SCORE_BATCH_SIZE) {
      const slice = rows.slice(i, i + SCORE_BATCH_SIZE);

      const batchInput = slice.map((r) => ({
        title: r.title ?? "",
        description: r.description ?? "",
        ticker: (r.tickers && r.tickers[0]) || null,
        published_utc: r.published_utc ?? null,
      }));

      const scored = await scoreBatchGemini(batchInput);

      // 룰 기반 보정 병행
      scored.forEach((s, j) => {
        const row = slice[j];
        const srcKind = sourceKindFromUrl(row.article_url);
        const evtKind = eventKindFromKeywordsOrText(row.keywords || [], `${row.title ?? ""} ${row.description ?? ""}`);

        const { confidence_rule } = pseudoCalibrate({
          sentiment_score: s.sentiment_score,
          confidence_model: s.confidence,
          source: srcKind,
          event: evtKind,
          // short_return_abs: 0.0 // 있으면 넣기
        });

        row.sentiment_score = s.sentiment_score;
        row.sentiment_confidence_model = s.confidence;
        row.sentiment_confidence_rule = confidence_rule;
        row.sentiment_reasoning = s.reasoning_summary;
      });

      console.log(`[SCORE] Scored rows ${i} ~ ${Math.min(i + SCORE_BATCH_SIZE - 1, rows.length - 1)}`);
    }
  } else {
    console.log(`[SCORE] Skipped (ENABLE_SCORING=false).`);
  }

  // DB 저장
  await insertChunked(rows);
  await sendDiscord();
  console.log(`[OK] inserted=${rows.length} window=${gteISO}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
