// Ruby EBTE — GitHub Actions edition.
// This is NOT the old always-on server. It is a short script meant to be run once every
// ~15 minutes by a GitHub Actions schedule, do one full scan+decide+trade cycle, save its
// state back into this repo, write a dashboard snapshot into docs/, and exit.
//
// WHAT THIS KEEPS from the old engine: Binance Demo Futures signed execution, Coinalyze
// funding/OI/liquidation snapshot, the 4 trading-session windows, a funding-crowding
// checklist signal, risk-based position sizing, and — importantly — REAL resting
// stop-loss/take-profit orders placed on Binance itself, so a position is protected even
// between cycles, not just while this script happens to be running.
//
// WHAT THIS DOES NOT (yet) REPLICATE from the old 3000-line engine: continuous WebSocket
// order-book/microstructure analysis (30-second spoofing/large-print detection needs a
// live stream, not a periodic snapshot), and the shadow-learning calibration system.
// Those needed a server that never stops; this deliberately isn't one, because every truly
// free host we tried either wanted a card or only kept a public URL alive for ~60 minutes.
// Tell me if/when you want either of those features rebuilt for this periodic model.

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

const ROOT = process.cwd();
const STATE_PATH = path.join(ROOT, "data", "state.json");
const DOCS_DIR = path.join(ROOT, "docs");
const BINANCE_EXEC_BASE = "https://demo-fapi.binance.com"; // orders/account: DEMO ONLY
const BINANCE_MARKET_BASE = "https://fapi.binance.com"; // price discovery: real public market
const HMAC_API_KEY = String(process.env.BINANCE_DEMO_KEY || "");
const HMAC_API_SECRET = String(process.env.BINANCE_DEMO_SECRET || "");
const COINALYZE_KEY = String(process.env.COINALYZE_API_KEY || "");
const GEMINI_KEY = String(process.env.GEMINI_API_KEY || "");

const MAX_OPEN_POSITIONS = 5;
const RISK_PERCENT_PER_TRADE = 1.0; // % of available balance risked per new trade
const LEVERAGE = 5;
const STOP_ATR_MULT = 1.2;
const TARGET_ATR_MULT = 2.0;
const CANDIDATE_POOL_SIZE = 25; // top movers by |24h change%| to consider each cycle
const MAJORS_FOR_SESSION_BIAS = ["BTCUSDT_PERP.A", "ETHUSDT_PERP.A", "SOLUSDT_PERP.A", "BNBUSDT_PERP.A"];

const nowMs = () => Date.now();
const isoNow = () => new Date().toISOString();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function safeJson(s, fallback) { try { return JSON.parse(s); } catch { return fallback; } }

// ---------- state ----------
function loadState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_PATH, "utf8"));
  } catch {
    return {
      createdAt: isoNow(),
      equityCurve: [],
      tradeLog: [],
      decisionLog: [],
      openPositions: {}, // symbol -> {side, entryPrice, qty, stopPrice, targetPrice, openedAt}
      lastRunAt: null,
      lastError: null,
      timeOffsetMs: 0,
    };
  }
}
function saveState(state) {
  fs.mkdirSync(path.dirname(STATE_PATH), { recursive: true });
  fs.writeFileSync(STATE_PATH, JSON.stringify(state, null, 2));
}
function pushCapped(arr, items, cap) {
  // `items` can be a single value or an array — always normalize to an array so every
  // decision/trade in a cycle actually gets recorded, not just the first one.
  for (const it of Array.isArray(items) ? items : [items]) arr.push(it);
  while (arr.length > cap) arr.shift();
}

// ---------- trading sessions (same UTC windows as the server engine) ----------
const TRADING_SESSIONS = {
  ASIAN: { startUtc: 0, endUtc: 9 },
  MIDDLE_EASTERN: { startUtc: 5, endUtc: 14 },
  LONDON_EUROPEAN: { startUtc: 7, endUtc: 16 },
  NEW_YORK_US: { startUtc: 13, endUtc: 22 },
};
function activeTradingSessions(d = new Date()) {
  const h = d.getUTCHours() + d.getUTCMinutes() / 60, active = [];
  for (const [name, w] of Object.entries(TRADING_SESSIONS)) {
    const open = w.startUtc < w.endUtc ? h >= w.startUtc && h < w.endUtc : h >= w.startUtc || h < w.endUtc;
    if (open) active.push(name);
  }
  return active;
}

// ---------- Binance Demo signed requests ----------
function signHmac(payload) { return crypto.createHmac("sha256", HMAC_API_SECRET).update(payload).digest("hex"); }
async function signedRequest(method, pathname, params = {}, timeOffsetMs = 0) {
  if (!HMAC_API_KEY || !HMAC_API_SECRET) throw new Error("BINANCE_DEMO_KEY/SECRET missing");
  const p = { ...params, timestamp: nowMs() + timeOffsetMs, recvWindow: 10000 };
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(p)) if (v !== undefined && v !== null) q.set(k, String(v));
  const payload = q.toString();
  q.set("signature", signHmac(payload));
  const r = await fetch(`${BINANCE_EXEC_BASE}${pathname}?${q}`, {
    method,
    headers: { "X-MBX-APIKEY": HMAC_API_KEY, "user-agent": "Ruby-EBTE-GHA/6.0" },
  });
  const body = await r.text();
  if (!r.ok) throw new Error(`Binance signed ${r.status}: ${body.slice(0, 300)}`);
  return body ? safeJson(body, {}) : {};
}
async function syncTimeOffset() {
  const t0 = nowMs();
  const r = await fetch(`${BINANCE_EXEC_BASE}/fapi/v1/time`);
  const s = await r.json();
  const t1 = nowMs();
  return Number(s.serverTime) - Math.floor((t0 + t1) / 2);
}
async function publicMarket(pathname, params = {}) {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== null) q.set(k, String(v));
  const r = await fetch(`${BINANCE_MARKET_BASE}${pathname}${q.size ? "?" + q : ""}`, { headers: { "user-agent": "Ruby-EBTE-GHA/6.0" } });
  const body = await r.text();
  if (!r.ok) throw new Error(`Binance public ${r.status}: ${body.slice(0, 300)}`);
  return body ? safeJson(body, {}) : {};
}

// ---------- Coinalyze ----------
async function coinalyzeSnapshot(symbols) {
  if (!COINALYZE_KEY) return null;
  try {
    const list = symbols.join(","), qs = `symbols=${encodeURIComponent(list)}`, headers = { api_key: COINALYZE_KEY }, nowSec = Math.floor(Date.now() / 1000);
    const [frRes, oiRes, liqRes] = await Promise.all([
      fetch(`https://api.coinalyze.net/v1/funding-rate?${qs}`, { headers }),
      fetch(`https://api.coinalyze.net/v1/open-interest?${qs}`, { headers }),
      fetch(`https://api.coinalyze.net/v1/liquidation-history?${qs}&interval=1hour&from=${nowSec - 6 * 3600}&to=${nowSec}`, { headers }),
    ]);
    if (!frRes.ok || !oiRes.ok) throw new Error(`Coinalyze ${frRes.status}/${oiRes.status}`);
    const [funding, openInterest] = await Promise.all([frRes.json(), oiRes.json()]);
    let liquidations = [];
    try { if (liqRes.ok) liquidations = await liqRes.json(); } catch {}
    return { ok: true, funding, openInterest, liquidations, fetchedAt: isoNow() };
  } catch (e) {
    return { ok: false, error: String(e).slice(0, 180) };
  }
}
function fundingBias(symbolFunding) {
  // positive & high -> shorts are being paid / longs crowded -> SHORT bias
  // negative & high -> longs are being paid / shorts crowded -> LONG bias
  const pct = +((+symbolFunding || 0) * 100).toFixed(4);
  if (pct > 0.05) return { bias: "SHORT", pct };
  if (pct < -0.05) return { bias: "LONG", pct };
  return { bias: "NEUTRAL", pct };
}

// ---------- candidate scan ----------
async function topMovers() {
  const tickers = await publicMarket("/fapi/v1/ticker/24hr");
  return (Array.isArray(tickers) ? tickers : [])
    .filter((t) => String(t.symbol || "").endsWith("USDT") && !/_/.test(t.symbol))
    .map((t) => ({ symbol: t.symbol, changePct: +t.priceChangePercent || 0, lastPrice: +t.lastPrice || 0, quoteVolume: +t.quoteVolume || 0 }))
    .filter((t) => t.quoteVolume > 5_000_000) // liquidity floor, avoid illiquid junk
    .sort((a, b) => Math.abs(b.changePct) - Math.abs(a.changePct))
    .slice(0, CANDIDATE_POOL_SIZE);
}
async function simpleAtr(symbol) {
  // crude 24x1h-candle average true range, used only to size the stop distance
  try {
    const kl = await publicMarket("/fapi/v1/klines", { symbol, interval: "1h", limit: 24 });
    if (!Array.isArray(kl) || !kl.length) return null;
    const trs = kl.map((c) => +c[2] - +c[3]); // high-low per candle, good enough for a stop-distance estimate
    const atr = trs.reduce((a, b) => a + b, 0) / trs.length;
    const lastClose = +kl[kl.length - 1][4];
    return { atr, lastClose };
  } catch {
    return null;
  }
}

// ---------- dashboard ----------
function writeDashboard(state, meta) {
  fs.mkdirSync(DOCS_DIR, { recursive: true });
  fs.writeFileSync(path.join(DOCS_DIR, ".nojekyll"), "");
  const data = {
    generatedAt: isoNow(),
    activeSessions: meta.activeSessions,
    account: meta.account,
    openPositions: state.openPositions,
    recentDecisions: state.decisionLog.slice(-25).reverse(),
    recentTrades: state.tradeLog.slice(-25).reverse(),
    equityCurve: state.equityCurve.slice(-200),
    lastError: state.lastError,
    coinalyzeConfigured: Boolean(COINALYZE_KEY),
    disclaimer: "Demo trading only. Informational dashboard, not financial advice.",
  };
  fs.writeFileSync(path.join(DOCS_DIR, "data.json"), JSON.stringify(data, null, 2));
  if (!fs.existsSync(path.join(DOCS_DIR, "index.html"))) {
    fs.writeFileSync(path.join(DOCS_DIR, "index.html"), DASHBOARD_HTML);
  }
}
const DASHBOARD_HTML = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover"/>
<title>Ruby EBTE — Dashboard</title>
<style>
:root{--bg:#0b0f14;--card:#12181f;--text:#e8edf2;--muted:#8b98a5;--good:#28c76f;--bad:#ea5455;--accent:#2f8cff;color-scheme:dark;}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:ui-monospace,Menlo,Consolas,monospace;padding:16px;}
h1{font-size:18px;margin:0 0 4px}
.sub{color:var(--muted);font-size:12px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px;margin-bottom:16px}
.card{background:var(--card);border-radius:10px;padding:14px;overflow-x:auto}
.card h2{font-size:13px;color:var(--muted);margin:0 0 8px;text-transform:uppercase;letter-spacing:.04em}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:4px 6px;text-align:left;border-bottom:1px solid #1e2630;white-space:nowrap}
.pos{color:var(--good)} .neg{color:var(--bad)}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;background:#1e2630;margin-right:4px}
.pill.on{background:#143a24;color:var(--good)}
</style></head>
<body>
<h1>Ruby EBTE</h1>
<div class="sub" id="meta">loading…</div>
<div class="grid">
  <div class="card"><h2>Active Sessions</h2><div id="sessions"></div></div>
  <div class="card"><h2>Account</h2><div id="account"></div></div>
  <div class="card"><h2>Open Positions</h2><div id="positions"></div></div>
</div>
<div class="grid">
  <div class="card"><h2>Recent Decisions</h2><div id="decisions"></div></div>
  <div class="card"><h2>Recent Trades</h2><div id="trades"></div></div>
</div>
<script>
async function load(){
  try{
    const r = await fetch('data.json?_=' + Date.now());
    const d = await r.json();
    document.getElementById('meta').textContent = 'Last run: ' + d.generatedAt + (d.lastError ? ' · last error: ' + d.lastError : '');
    document.getElementById('sessions').innerHTML = (d.activeSessions||[]).map(s=>'<span class="pill on">'+s+'</span>').join('') || '<span class="pill">none open</span>';
    const a = d.account||{};
    document.getElementById('account').innerHTML = '<table><tr><td>Balance</td><td>'+(a.balance??'—')+'</td></tr><tr><td>Unrealized PnL</td><td>'+(a.unrealizedPnl??'—')+'</td></tr></table>';
    const pos = Object.entries(d.openPositions||{});
    document.getElementById('positions').innerHTML = pos.length ? '<table><tr><th>Symbol</th><th>Side</th><th>Entry</th><th>Stop</th><th>Target</th></tr>' + pos.map(([sym,p])=>'<tr><td>'+sym+'</td><td>'+p.side+'</td><td>'+p.entryPrice+'</td><td>'+p.stopPrice+'</td><td>'+p.targetPrice+'</td></tr>').join('') + '</table>' : '<span style="color:var(--muted)">none</span>';
    document.getElementById('decisions').innerHTML = (d.recentDecisions||[]).map(x=>'<div style="margin-bottom:6px;font-size:12px"><b>'+x.time+'</b> '+x.symbol+' — '+x.action+' <span style="color:var(--muted)">'+ (x.reason||'') +'</span></div>').join('') || '<span style="color:var(--muted)">none yet</span>';
    document.getElementById('trades').innerHTML = (d.recentTrades||[]).map(x=>'<div style="margin-bottom:6px;font-size:12px"><b>'+x.time+'</b> '+x.symbol+' '+x.side+' qty '+x.qty+'</div>').join('') || '<span style="color:var(--muted)">none yet</span>';
  }catch(e){ document.getElementById('meta').textContent = 'Failed to load data.json: ' + e; }
}
load(); setInterval(load, 30000);
</script>
</body></html>`;

// ---------- main cycle ----------
async function main() {
  const state = loadState();
  const decisions = [];
  let account = null;

  try {
    const timeOffsetMs = await syncTimeOffset();
    state.timeOffsetMs = timeOffsetMs;

    if (HMAC_API_KEY && HMAC_API_SECRET) {
      const acc = await signedRequest("GET", "/fapi/v3/account", {}, timeOffsetMs);
      account = { balance: +acc.totalWalletBalance || 0, unrealizedPnl: +acc.totalUnrealizedProfit || 0, availableBalance: +acc.availableBalance || 0 };
    }

    const sessions = activeTradingSessions();
    const coinalyze = await coinalyzeSnapshot(MAJORS_FOR_SESSION_BIAS);
    const movers = await topMovers().catch(() => []);

    const openCount = Object.keys(state.openPositions || {}).length;
    const canOpenMore = sessions.length > 0 && openCount < MAX_OPEN_POSITIONS && account && account.availableBalance > 0;

    if (!canOpenMore) {
      decisions.push({ time: isoNow(), symbol: "-", action: "SKIP_SCAN", reason: sessions.length === 0 ? "no trading session open" : openCount >= MAX_OPEN_POSITIONS ? "max open positions reached" : "no account/balance data" });
    } else {
      // Only look at movers we can also get a funding read on, so the decision is backed by
      // the checklist's funding-crowding signal, not momentum alone.
      const coinalyzeSymbolFor = (sym) => `${sym}_PERP.A`;
      for (const m of movers) {
        if (state.openPositions[m.symbol]) continue; // already in a position on this symbol
        if (Object.keys(state.openPositions).length >= MAX_OPEN_POSITIONS) break;

        let fundingRow = null;
        if (coinalyze?.ok) {
          fundingRow = (coinalyze.funding || []).find((f) => f.symbol === coinalyzeSymbolFor(m.symbol));
        }
        if (!fundingRow) continue; // no funding coverage for this symbol this cycle — skip rather than guess

        const { bias } = fundingBias(fundingRow.value);
        if (bias === "NEUTRAL") continue;
        // Contrarian confirmation: only take the funding-crowding signal if price has moved
        // the way the crowd was betting (i.e. the squeeze thesis), not against it yet.
        const momentumAgrees = (bias === "SHORT" && m.changePct > 0) || (bias === "LONG" && m.changePct < 0);
        if (!momentumAgrees) continue;

        const atrInfo = await simpleAtr(m.symbol);
        if (!atrInfo || !atrInfo.atr) continue;
        const entryPrice = atrInfo.lastClose;
        const stopDist = atrInfo.atr * STOP_ATR_MULT;
        const targetDist = atrInfo.atr * TARGET_ATR_MULT;
        const side = bias === "LONG" ? "BUY" : "SELL";
        const stopPrice = side === "BUY" ? entryPrice - stopDist : entryPrice + stopDist;
        const targetPrice = side === "BUY" ? entryPrice + targetDist : entryPrice - targetDist;

        const riskUsdt = (account.availableBalance * RISK_PERCENT_PER_TRADE) / 100;
        const qtyRaw = riskUsdt / stopDist;
        const qty = Math.max(0, +qtyRaw.toFixed(3));
        if (!qty) continue;

        try {
          await signedRequest("POST", "/fapi/v1/marginType", { symbol: m.symbol, marginType: "ISOLATED" }, state.timeOffsetMs).catch(() => {});
          await signedRequest("POST", "/fapi/v1/leverage", { symbol: m.symbol, leverage: LEVERAGE }, state.timeOffsetMs).catch(() => {});
          await signedRequest("POST", "/fapi/v1/order", { symbol: m.symbol, side, type: "MARKET", quantity: qty, newOrderRespType: "RESULT" }, state.timeOffsetMs);
          const closeSide = side === "BUY" ? "SELL" : "BUY";
          await signedRequest("POST", "/fapi/v1/order", { symbol: m.symbol, side: closeSide, type: "STOP_MARKET", stopPrice: stopPrice.toFixed(6), closePosition: "true", workingType: "MARK_PRICE" }, state.timeOffsetMs);
          await signedRequest("POST", "/fapi/v1/order", { symbol: m.symbol, side: closeSide, type: "TAKE_PROFIT_MARKET", stopPrice: targetPrice.toFixed(6), closePosition: "true", workingType: "MARK_PRICE" }, state.timeOffsetMs);

          state.openPositions[m.symbol] = { side, entryPrice, qty, stopPrice: +stopPrice.toFixed(6), targetPrice: +targetPrice.toFixed(6), openedAt: isoNow() };
          pushCapped(state.tradeLog, { time: isoNow(), symbol: m.symbol, side, qty, entryPrice }, 500);
          decisions.push({ time: isoNow(), symbol: m.symbol, action: `ENTER_${side}`, reason: `funding bias ${bias} (${fundingRow.value}) + momentum agrees (${m.changePct}%)` });
        } catch (e) {
          decisions.push({ time: isoNow(), symbol: m.symbol, action: "ENTER_FAILED", reason: String(e).slice(0, 200) });
        }
      }
      if (!decisions.length) decisions.push({ time: isoNow(), symbol: "-", action: "NO_SIGNAL", reason: "no mover had a funding-confirmed checklist signal this cycle" });
    }

    // Reconcile: drop any locally-tracked position that Binance no longer shows (closed by
    // its own resting stop/TP order since the last cycle).
    if (HMAC_API_KEY && HMAC_API_SECRET) {
      try {
        const risk = await signedRequest("GET", "/fapi/v3/positionRisk", {}, state.timeOffsetMs);
        const liveSymbols = new Set((Array.isArray(risk) ? risk : []).filter((p) => Math.abs(+p.positionAmt) > 0).map((p) => p.symbol));
        for (const sym of Object.keys(state.openPositions)) {
          if (!liveSymbols.has(sym)) {
            decisions.push({ time: isoNow(), symbol: sym, action: "CLOSED", reason: "no longer open on Binance (stop/target hit, or closed manually)" });
            delete state.openPositions[sym];
          }
        }
      } catch (e) {
        decisions.push({ time: isoNow(), symbol: "-", action: "RECONCILE_FAILED", reason: String(e).slice(0, 200) });
      }
    }

    pushCapped(state.decisionLog, decisions.length ? decisions : [{ time: isoNow(), symbol: "-", action: "NOOP", reason: "nothing to do" }], 500);
    if (account) pushCapped(state.equityCurve, { time: isoNow(), balance: account.balance }, 2000);
    state.lastError = null;
    state.lastRunAt = isoNow();
    saveState(state);
    writeDashboard(state, { activeSessions: sessions, account });
    console.log(`Cycle complete. Sessions: ${sessions.join(",") || "none"}. Decisions: ${decisions.length}. Open positions: ${Object.keys(state.openPositions).length}.`);
  } catch (e) {
    state.lastError = String(e && e.stack || e).slice(0, 600);
    state.lastRunAt = isoNow();
    saveState(state);
    writeDashboard(state, { activeSessions: activeTradingSessions(), account });
    console.error("Cycle FAILED:", state.lastError);
    process.exitCode = 1;
  }
}

main();
