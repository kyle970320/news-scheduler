import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

// ===== env =====
const API_BASE = process.env.NEWS_API_BASE;
const API_KEY  = process.env.NEWS_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE = process.env.SUPABASE_SERVICE_ROLE;
const DISCORD_WEBHOOK= process.env.DISCORD_WEBHOOK;

if (!API_BASE || !SUPABASE_URL || !SUPABASE_SERVICE_ROLE) {
  process.exit(1);
}

const TABLE = "news";
const LIMIT = 300;

async function cleanupOldNews(days = 2) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - days);
  const cutoffISO = cutoff.toISOString();

  console.log(`[CLEANUP] Deleting records older than ${cutoffISO}`);

  const { error } = await supabase
    .from(TABLE)
    .delete()
    .lt("created_at", cutoffISO); // published_utc이 cutoff보다 작은 데이터 삭제

  if (error) throw error;
}

async function sendDiscord() {
  if (!DISCORD_WEBHOOK) {
  console.error("❌ Missing DISCORD_WEBHOOK_URL");
  return
}
  const payload = {
    username: "호외요 호외",
    content: '최신 뉴스가 갱신되었습니다!'
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

// ===== helpers =====
const toISO = (d) => new Date(d).toISOString();

function lastHourWindow() {
  const now = new Date();
  const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);
  return { gteISO: toISO(oneHourAgo) };
}

// ===== fetch from API (NO keyword/sentiment) =====
async function fetchNews({ gteISO, ticker = null }) {
  const url = new URL("/v2/reference/news", API_BASE);
  url.searchParams.set("sort", "published_utc");
  url.searchParams.set("order", "asc");
  url.searchParams.set("limit", LIMIT);
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

// ===== map to your table schema exactly =====
function mapToRows(items) {
  return items.map((r) => {
    const title = r.title ?? null;
    const description = r.description ?? r.summary ?? null;
    const article_url = r.article_url ??  null;
    const published =
      r.published_utc ?? r.published_at ?? r.date ?? null;
    const tickers = Array.isArray(r.tickers) ? r.tickers : [];
    const insights = r?.insights ?? {}
    // API에 keywords/custom sentiment가 없으면 빈 값으로
    const keywords = Array.isArray(r.keywords) ? r.keywords : [];


    return {
      title,
      description,
      article_url,
      keywords,
      published_utc: published ? new Date(published).toISOString() : null,
      insights,
      tickers,
    };
  });
}

// ===== insert to Supabase (chunked) =====
const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE, {
  auth: { persistSession: false }
});

async function insertChunked(rows, chunkSize = 100) {
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const { error } = await supabase.from(TABLE).insert(chunk);
    if (error) throw error;
  }
}

async function main() {
  const { gteISO } = lastHourWindow();
  
  //오래된 뉴스 정리
  await cleanupOldNews(2);
  
  const items = await fetchNews({ gteISO });     // 한 번에 300 요청
  if (!items?.length) {
    console.log(`[OK] No items. window=${gteISO}`);
    return;
  }
  const rows = mapToRows(items);
  await insertChunked(rows);
  await sendDiscord();
  console.log(`[OK] inserted=${rows.length} window=${gteISO}`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});