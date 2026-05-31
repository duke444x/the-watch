// =============================================================================
// watch-monitor.js — THE WATCH · intra-period monitor (5-minute loop)   [v2]
//
// Watches the levels Capt armed in his full reads against the live Kraken tape
// and acts on the opportunistic side between reads — the dip to a stack target
// at 2AM that used to be missed until 9AM.
//
// TWO MODES (env MONITOR_MODE):
//   detect (default) — logs what it WOULD do; touches nothing.
//   live             — on a qualified ENTRY trigger, runs the bounded mini-read
//                      (watch-miniread.js) and, on a "buy", fills a small capped
//                      paper add, records it as a lightweight 'monitor' run (so it
//                      rides the existing attest → /run/N → Reckoning machinery),
//                      and posts to #capts-ledger.
//
// EXITS stay on the existing mechanical exit-check (invalidation is a hard stop —
// never routed through an LLM). This loop only LOGS exit conditions for visibility.
//
// GUARDRAILS (live): arm-expiry (skip stale levels), cooldown + 1% reawaken
// (no mini-read spam while price camps a level), re-target leash (≤2 then park),
// per-fill cap (small rail slice, hard USD ceiling), and a daily per-pair fill cap.
//
// PM2:  pm2 start watch-monitor.js --name watch-monitor && pm2 save
// Go live: set MONITOR_MODE=live in .env, then  pm2 restart watch-monitor --update-env
// =============================================================================

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import Ledger from './ledger.js';
import { postAdminEvent, postTradeEvent } from './webhooks.js';
import { attestRunNow, sweepPendingAttestations } from './attest.js';
import { runMiniRead } from './watch-miniread.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, '.env') });

const MODE        = (process.env.MONITOR_MODE || 'detect').toLowerCase();           // 'detect' | 'live'
const POLL_MS     = Number(process.env.MONITOR_POLL_MS)        || 5 * 60 * 1000;     // 5 min
const RENOTIFY_MS = Number(process.env.MONITOR_RENOTIFY_MS)    || 60 * 60 * 1000;    // detect dedupe
const ARM_EXPIRY_H= Number(process.env.MONITOR_ARM_EXPIRY_H)   || 36;               // skip levels older than this
const COOLDOWN_MS = Number(process.env.MONITOR_COOLDOWN_MS)    || 30 * 60 * 1000;    // after a hold/act
const REAWAKEN_PCT= Number(process.env.MONITOR_REAWAKEN_PCT)   || 0.01;             // 1% move breaks cooldown
const LEASH_MAX   = Number(process.env.MONITOR_LEASH_MAX)      || 2;                // re-targets before parking
const ADD_PCT     = Number(process.env.MONITOR_ADD_PCT)        || 0.05;             // per-fill size (rail-ish)
const MAX_FILL_USD= Number(process.env.MONITOR_MAX_FILL_USD)   || 600;             // hard per-fill ceiling
const MAX_FILLS_DAY = Number(process.env.MONITOR_MAX_FILLS_PER_PAIR_DAY) || 2;     // runaway guard
const LEDGER_DB   = path.join(__dirname, 'data', 'ledger.db');

const TIER = { HBARUSD: 'deep', BTCUSD: 'deep', SOLUSD: 'deep', SUIUSD: 'deep', DOGUSD: 'moderate' };

const ledger = new Ledger(LEDGER_DB);
const _lastNotified = new Map();   // detect dedupe: key -> ms
const _cooldown     = new Map();   // live: thesisId -> { until, px }
const _retargets    = new Map();   // live: thesisId -> count
const _fillsToday   = new Map();   // live: 'PAIR:YYYY-MM-DD' -> count

const stamp = () => new Date().toISOString();
const log   = (...a) => console.log(`[monitor ${stamp()}]`, ...a);
const today = () => new Date().toISOString().slice(0, 10);

// ── thin kraken reader (mirrors watch.js) ────────────────────────────────────
function runKraken(args) {
  return new Promise((resolve, reject) => {
    const proc = spawn('kraken', args, { shell: false });
    let stdout = '', stderr = '';
    proc.stdout.on('data', d => { stdout += d; });
    proc.stderr.on('data', d => { stderr += d; });
    proc.on('close', code => {
      if (code !== 0) return reject(new Error(`kraken ${args.join(' ')} exited ${code}: ${stderr.trim()}`));
      try { resolve(JSON.parse(stdout)); } catch (e) { reject(new Error(`parse: ${e.message}`)); }
    });
    proc.on('error', e => reject(new Error(`spawn kraken: ${e.message}`)));
  });
}
function pickPair(data, pair) {
  if (data[pair]) return data[pair];
  if (pair === 'BTCUSD') return data['XXBTZUSD'] || data['XBTUSD'] || null;
  return null;
}
async function fetchPrices(pairs) {
  if (!pairs.length) return {};
  const data = await runKraken(['ticker', ...pairs, '-o', 'json']);
  const out = {};
  for (const p of pairs) {
    const pd = pickPair(data, p);
    if (pd && pd.c) out[p] = parseFloat(pd.c[0]);
  }
  return out;
}

const hoursSince = tsUtc => { const t = Date.parse(tsUtc); return Number.isFinite(t) ? (Date.now() - t) / 3.6e6 : 0; };

async function notify(key, title, desc) {       // detect-mode #watch-admin, deduped
  log(title, '·', desc);
  const now = Date.now();
  if (now - (_lastNotified.get(key) || 0) < RENOTIFY_MS) return;
  _lastNotified.set(key, now);
  await admin(title, desc);
}
async function admin(title, desc) {
  try { await postAdminEvent('info', `[watcher:${MODE}] ${title}`, desc, [], 'watch-monitor.js'); }
  catch (e) { log('admin notify failed:', e.message); }
}
function setCooldown(key, px, ms = COOLDOWN_MS) { _cooldown.set(String(key), { until: Date.now() + ms, px }); }
function inCooldown(key, px) {
  const cd = _cooldown.get(String(key));
  if (!cd) return false;
  if (Date.now() >= cd.until) return false;
  return Math.abs(px - cd.px) / cd.px < REAWAKEN_PCT;   // a 1% move breaks it early
}

// ── live: execute a capped paper add + record as a 'monitor' run + anchor ────
async function executeAndRecord(w, dec) {
  const pair = w.pair;
  let acct;
  try { acct = (await runKraken(['paper', 'status', '-o', 'json'])).current_value; }
  catch (e) { log(`  buy aborted ${pair}: paper status failed: ${e.message}`); return; }

  let price;
  try { const pd = pickPair(await runKraken(['ticker', pair, '-o', 'json']), pair); price = parseFloat(pd.c[0]); }
  catch (e) { log(`  buy aborted ${pair}: ticker failed: ${e.message}`); return; }

  const usd = Math.min(acct * ADD_PCT, MAX_FILL_USD);
  const volume = (usd / price).toFixed(4);

  let result;
  try { result = await runKraken(['paper', 'buy', pair, volume, '-o', 'json']); }
  catch (e) { log(`  buy FAILED ${pair}: ${e.message}`); await admin(`buy FAILED ${pair}`, e.message); return; }

  const execution = {
    pair, side: 'buy', size: 'rail', plank: 'the rail',
    volume: parseFloat(volume), symbol: pair.replace('USD', ''),
    tier: TIER[pair] || 'deep',
    fillPrice: result.price, cost: result.cost, fee: result.fee,
    orderId: result.order_id, thesis: dec.reason, confidence: 'monitor', forced: false,
  };
  const levels = { invalidation_price: dec.invalidation, take_profit_price: dec.take_profit, time_stop_hours: 48 };

  let runId = null, tradeId = null;
  try {
    runId = ledger.startRun('monitor');
    const decisionId = ledger.recordDecision(runId,
      { action: 'enter', pair, side: 'buy', size: 'rail', thesis: dec.reason, confidence: 'monitor', forced: false },
      execution.tier);
    tradeId = ledger.recordTrade(runId, decisionId, execution, levels);
    const narration =
      `⚓ Intra-period add — ${pair}\n\n${dec.reason}\n\n` +
      `Filled rail slice: ${execution.volume} ${execution.symbol} @ $${execution.fillPrice} ` +
      `(cost $${Number(execution.cost).toFixed(2)}), invalidation ${dec.invalidation ?? '—'}.\n\n` +
      `— Capt. Crawl · Watch monitor`;
    ledger.recordBridgeLog(runId, narration);
    ledger.resolveThesis(w.thesis_id, tradeId, runId);     // retire the level so it won't re-fire
    ledger.completeRun(runId, 0);
  } catch (e) {
    log(`  ledger record failed ${pair}: ${e.message}`);
  }

  try { if (runId) await attestRunNow(ledger, runId, { timeoutMs: 20000 }); }
  catch (e) { log(`  attest failed ${pair}: ${e.message}`); }

  try { await postTradeEvent(execution, runId, levels); }
  catch (e) { log(`  trade-event post failed ${pair}: ${e.message}`); }

  log(`  FILLED ${pair}: ${execution.volume} @ ${execution.fillPrice} · run ${runId} · trade ${tradeId}`);
  await admin(`FILLED ${pair} (rail)`, `${execution.volume} @ $${execution.fillPrice} · inval ${dec.invalidation ?? '—'} · captsledger.com/run/${runId}`);
}

// ── live: handle one qualified entry trigger ─────────────────────────────────
// Summarize open trades on a pair into a single position view for the mini-read.
// One trade -> passed through; multiple -> summed volume, volume-weighted entry,
// and the NEAREST stop (highest invalidation = first to trigger on a drop).
function summarizeHeld(rows) {
  if (!rows || rows.length === 0) return null;
  if (rows.length === 1) {
    const r = rows[0];
    return { volume: r.volume, fill_price: r.fill_price, invalidation_price: r.invalidation_price ?? null };
  }
  let vol = 0, notional = 0, inval = null;
  for (const r of rows) {
    const v = Number(r.volume) || 0;
    vol += v;
    notional += v * (Number(r.fill_price) || 0);
    if (r.invalidation_price != null) {
      inval = (inval === null) ? Number(r.invalidation_price) : Math.max(inval, Number(r.invalidation_price));
    }
  }
  return {
    volume: Number(vol.toFixed(4)),
    fill_price: vol > 0 ? Number((notional / vol).toFixed(6)) : null,
    invalidation_price: inval,
  };
}

async function handleEntryTrigger(w, px) {
  const key = String(w.thesis_id);

  const ageH = hoursSince(w.ts_utc);
  if (ageH > ARM_EXPIRY_H) { log(`  skip #${w.thesis_id} ${w.pair}: stale (armed ${ageH.toFixed(0)}h ago > ${ARM_EXPIRY_H}h)`); return; }
  if (inCooldown(key, px))  { log(`  skip #${w.thesis_id} ${w.pair}: cooldown`); return; }

  let dec;
  // portfolio-aware: let Capt see existing exposure on this pair before adding.
  const _held = summarizeHeld(
    (typeof ledger.getOpenTradesByPair === 'function')
      ? ledger.getOpenTradesByPair(w.pair)
      : (ledger.getOpenTrades() || []).filter(t => t.pair === w.pair));
  try { dec = await runMiniRead({ pair: w.pair, triggerKind: 'entry', level: (w.level_high ?? w.level_low), thesisNote: w.note, position: _held }); }
  catch (e) { log(`  mini-read failed #${w.thesis_id} ${w.pair}: ${e.message} — treating as hold`); setCooldown(key, px); return; }

  log(`  mini-read #${w.thesis_id} ${w.pair}: ${dec.action.toUpperCase()} — ${dec.reason}`);

  // Reckoning v2: capture that the level was tagged (decoupled from any fill),
  // once per thesis, logging Capt's stand-down reasoning as its own monitor run.
  if (dec.action !== 'buy' && w.reached_run_id == null) {
    try {
      const reachRun = ledger.startRun('monitor');
      ledger.recordBridgeLog(reachRun,
        `⚓ Level tagged, stood down — ${w.pair}\n\n${dec.reason}\n\n— Capt. Crawl · Watch monitor (reach, no fill)`);
      ledger.markThesisReached(w.thesis_id, reachRun);
      ledger.completeRun(reachRun, 0);
      try { await attestRunNow(ledger, reachRun, { timeoutMs: 20000 }); }
      catch (e) { log(`  reach attest failed #${w.thesis_id} ${w.pair}: ${e.message}`); }
    } catch (e) { log(`  reach-capture failed #${w.thesis_id} ${w.pair}: ${e.message}`); }
  }

  if (dec.action === 'buy') {
    const fk = `${w.pair}:${today()}`;
    if ((_fillsToday.get(fk) || 0) >= MAX_FILLS_DAY) {
      log(`  cap: ${w.pair} already ${MAX_FILLS_DAY} monitor fills today — skipping`);
      await admin(`fill cap ${w.pair}`, `${MAX_FILLS_DAY}/day reached; standing down until tomorrow or next full read`);
      setCooldown(key, px); return;
    }
    await executeAndRecord(w, dec);
    _fillsToday.set(fk, (_fillsToday.get(fk) || 0) + 1);
    setCooldown(key, px);
  } else if (dec.action === 'retarget') {
    const n = (_retargets.get(key) || 0) + 1;
    _retargets.set(key, n);
    if (n > LEASH_MAX) {
      log(`  leash: #${w.thesis_id} ${w.pair} re-targeted ${n}x — parking until next full read`);
      await admin(`parked ${w.pair}`, `re-targeted ${n}x; leaving it to the next full read`);
      setCooldown(key, px, 12 * 3600 * 1000);   // long park
    } else {
      await admin(`retarget ${w.pair}`, `${dec.reason}${dec.new_level ? ` → new level ${dec.new_level}` : ''} (logged; next full read re-arms)`);
      setCooldown(key, px);
    }
  } else if (dec.action === 'rotate') {
    await admin(`rotate intent ${w.pair}`, `${dec.reason}${dec.rotate_to ? ` → ${dec.rotate_to}` : ''} (v1: logged, not auto-executed)`);
    setCooldown(key, px);
  } else {   // hold | pass
    if (dec.action === 'pass') await admin(`stand down ${w.pair}`, dec.reason);
    setCooldown(key, px);
  }
}

// ── one monitoring pass ──────────────────────────────────────────────────────
async function pass() {
  let openTrades = [], watching = [];

  // Reckoning v2: expire stale, unreached watched levels (the predicted move
  // never came within ARM_EXPIRY_H) so they get a terminal verdict and stop
  // polling. Before the fetch so getWatchingTheses excludes them this pass.
  try {
    const cutoffIso = new Date(Date.now() - ARM_EXPIRY_H * 3600 * 1000).toISOString();
    const nExpired = ledger.expireStaleTheses(cutoffIso);
    if (nExpired) log(`expired ${nExpired} stale watched level(s) (> ${ARM_EXPIRY_H}h, unreached)`);
  } catch (e) { log('expire-stale failed:', e.message); }

  // Auto-reconcile: confirm submitted/pending attestations against the mirror so
  // submitted -> confirmed settles within a pass or two, not at the next watch.
  // Cheap no-op when nothing's pending (local SELECT returns []); best-effort.
  try { await sweepPendingAttestations(ledger); }
  catch (e) { log('attestation sweep failed:', e.message); }
  try { openTrades = ledger.getOpenTrades() || []; }     catch (e) { log('getOpenTrades failed:', e.message); }
  try { watching   = ledger.getWatchingTheses() || []; } catch (e) { log('getWatchingTheses failed:', e.message); }

  const pairs = new Set();
  openTrades.forEach(t => pairs.add(t.pair));
  watching.forEach(w => pairs.add(w.pair));
  if (pairs.size === 0) { log('nothing armed (no open positions or watched levels)'); return; }

  let prices;
  try { prices = await fetchPrices([...pairs]); }
  catch (e) { log('price fetch failed:', e.message); return; }

  log(`pass: ${openTrades.length} position(s), ${watching.length} watched level(s) · mode=${MODE}`);

  // Open positions → EXIT conditions are detect-only here (the mechanical exit-check owns exits)
  for (const t of openTrades) {
    const px = prices[t.pair];
    if (px == null) continue;
    const tags = [];
    if (t.invalidation_price != null && px <= t.invalidation_price) tags.push(`INVALIDATION (≤ ${t.invalidation_price})`);
    if (t.take_profit_price  != null && px >= t.take_profit_price)  tags.push(`TAKE-PROFIT (≥ ${t.take_profit_price})`);
    const ageH = hoursSince(t.ts_utc);
    if (t.time_stop_hours != null && ageH >= t.time_stop_hours)     tags.push(`TIME-STOP (${ageH.toFixed(1)}h ≥ ${t.time_stop_hours}h)`);
    if (tags.length) {
      await notify(`exit:${t.trade_id}:${tags[0].split(' ')[0]}`, `EXIT CONDITION ${t.pair} (trade #${t.trade_id})`,
        `price ${px} · ${tags.join(' · ')} · handled by the mechanical exit-check`);
    } else {
      log(`  pos ${t.pair} #${t.trade_id}: px ${px} | inval ${t.invalidation_price ?? '—'} · tp ${t.take_profit_price ?? '—'} · age ${ageH.toFixed(1)}/${t.time_stop_hours}h — holding`);
    }
  }

  // Watching theses → ENTRY triggers (live: mini-read + execute; detect: log)
  for (const w of watching) {
    const px = prices[w.pair];
    if (px == null) continue;
    const level = (w.level_high != null) ? w.level_high : w.level_low;
    if (level == null) continue;
    const dir   = (w.direction || '').toLowerCase();
    const above = dir.includes('break') || dir.includes('above') || dir.includes('reclaim');
    const hit   = above ? (px >= level) : (px <= level);
    if (!hit) {
      log(`  watch ${w.pair} #${w.thesis_id}: px ${px} | zone ${above ? '≥' : '≤'} ${level} — not yet`);
      continue;
    }
    if (MODE === 'live') {
      await handleEntryTrigger(w, px);
    } else {
      await notify(`entry:${w.thesis_id}`, `WOULD ENTER/ADD ${w.pair} (thesis #${w.thesis_id})`,
        `price ${px} · zone ${above ? '≥' : '≤'} ${level}${w.note ? ` · "${w.note}"` : ''} · no action (detect)`);
    }
  }
}

function start() {
  log(`watch-monitor up · mode=${MODE} · poll=${POLL_MS / 1000}s · db=${LEDGER_DB}`);
  if (MODE === 'live') log(`LIVE: entry triggers will mini-read + fill (cap ${ADD_PCT * 100}%/$${MAX_FILL_USD}, ${MAX_FILLS_DAY}/pair/day, arm-expiry ${ARM_EXPIRY_H}h). Exits stay on the mechanical check.`);
  pass().catch(e => log('pass error:', e.message));
  setInterval(() => { pass().catch(e => log('pass error:', e.message)); }, POLL_MS);
}

start();
