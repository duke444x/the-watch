// =============================================================================
// watch-miniread.js — THE WATCH · bounded intra-period decision (the "brain")
//
// When the monitor sees a pre-armed level get tagged between full reads, it asks
// Capt for ONE focused call on THAT level only — not a fresh five-asset analysis.
// He already did the thinking when he armed the level; this is the fill-time
// sanity gate Duke wanted: honor it, wait a touch (we re-check in ~5 min),
// re-target it to something realistic, rotate, or pass.
//
// This module is PURE DECISION. It fetches a tight context (the triggered asset
// + BTC as the regime anchor) and returns a structured object. It does not touch
// the paper account — the monitor executes on the result.
//
//   action: 'buy' | 'hold' | 'retarget' | 'rotate' | 'pass'
//
// Scoped on purpose: single asset + BTC, ticker-grounded, one cheap call that
// only fires on a real trigger (rare). Uses the same model as the full read.
// =============================================================================

import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import Anthropic from '@anthropic-ai/sdk';

const MODEL = process.env.MINIREAD_MODEL || 'claude-opus-4-8';
// Load .env at module-eval, BEFORE constructing the client. ESM evaluates this
// module before the importing process (monitor/scheduler) runs its own
// dotenv.config(), so without this the client is built with no ANTHROPIC_API_KEY
// and every call throws "Could not resolve authentication method".
dotenv.config({ path: path.join(path.dirname(fileURLToPath(import.meta.url)), '.env') });
const anthropic = new Anthropic();   // reads ANTHROPIC_API_KEY from env

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

// Compact ticker read for one pair: last, intraday change, 24h range, activity.
async function tickerRead(pair) {
  const data = await runKraken(['ticker', pair, '-o', 'json']);
  const pd = pickPair(data, pair);
  if (!pd || !pd.c) throw new Error(`no ticker for ${pair}`);
  const last = parseFloat(pd.c[0]);
  const open = parseFloat(pd.o);
  return {
    last,
    changePct: open ? +(((last - open) / open) * 100).toFixed(2) : 0,
    high24: parseFloat(pd.h[1]),
    low24:  parseFloat(pd.l[1]),
    vol24:  Math.round(parseFloat(pd.v[1])),
    trades24: parseInt(pd.t[1], 10),
  };
}

// ── prompt ───────────────────────────────────────────────────────────────────
const SYSTEM = `You are Capt. Crawl running THE WATCH's intra-period check. This is NOT a full read — it is a single fill-time decision on ONE level you already armed in an earlier read. You are a paper-trading stacking agent whose mission is to end up holding MORE HBAR and DOG over time; BTC/SOL/SUI are vehicles. Same voice, same discipline as your Bridge Logs: clean, dry, no hype, honest invalidations.

The level you armed just got tagged. Decide ONE thing. You re-check every ~5 minutes, so "wait a touch" is cheap and legitimate.

Your decision space:
- "buy"      — conditions still hold; take the pre-armed add/entry now. This is a SLICE toward the core, not a full deploy.
- "hold"     — ambiguous or it's slicing through the level in a way you don't trust (falling knife). Don't fill yet; look again next pass.
- "retarget" — the original level isn't realistic anymore. Set a new, within-reach level. It MUST stay inside the original invalidation (don't chase forever).
- "rotate"   — a sibling is set up better; rotate intent toward another pair.
- "pass"     — the setup has decayed; stand down on this level for now.

Rules:
- A pre-armed level is YOUR prior committed decision. Default to honoring it unless something has materially changed (BTC regime broke, it's knifing through with no base, the book vanished).
- Never fill into freefall. A controlled pullback to the level is a buy; a violent slice through it is a "hold" or "retarget".
- Size stays small — "rail" only for intra-period adds.
- If you "buy", name an invalidation (a clean loss level below). Take-profit optional.
- Stacking targets are HBAR and DOG; you only add a vehicle (SOL/SUI/BTC) on a clean setup, and BTC's defended-floor template is frozen.

Output ONLY a JSON object, no prose, no markdown fences:
{"action":"buy|hold|retarget|rotate|pass","reason":"<one or two sentences, your voice>","size":"rail|null","invalidation":<number|null>,"take_profit":<number|null>,"new_level":<number|null>,"rotate_to":"<PAIR>|null"}`;

function buildUserPrompt({ pair, triggerKind, level, thesisNote, position }, asset, btc) {
  const lines = [];
  lines.push(`TRIGGER: ${pair} ${triggerKind} level ${level} just tagged.`);
  if (thesisNote) lines.push(`Your original note when you armed it: "${thesisNote}"`);
  if (position) {
    lines.push(`You already hold ${pair}: ${position.volume} @ ${position.fill_price} entry, invalidation ${position.invalidation_price ?? '—'}.`);
  }
  lines.push('');
  lines.push(`${pair} now: last ${asset.last}, intraday ${asset.changePct}%, 24h range ${asset.low24}–${asset.high24}, vol ${asset.vol24}, ${asset.trades24} trades.`);
  lines.push(`BTC regime: last ${btc.last}, intraday ${btc.changePct}%, 24h range ${btc.low24}–${btc.high24}.`);
  lines.push('');
  lines.push('Decide. JSON only.');
  return lines.join('\n');
}

function parseDecision(text) {
  const cleaned = String(text).replace(/```json|```/g, '').trim();
  const start = cleaned.indexOf('{');
  const end = cleaned.lastIndexOf('}');
  if (start === -1 || end === -1) throw new Error('no JSON object in mini-read response');
  const obj = JSON.parse(cleaned.slice(start, end + 1));
  const VALID = ['buy', 'hold', 'retarget', 'rotate', 'pass'];
  if (!VALID.includes(obj.action)) throw new Error(`invalid action: ${obj.action}`);
  return {
    action: obj.action,
    reason: String(obj.reason || '').slice(0, 600),
    size: obj.size === 'rail' ? 'rail' : null,
    invalidation: Number.isFinite(obj.invalidation) ? obj.invalidation : null,
    take_profit: Number.isFinite(obj.take_profit) ? obj.take_profit : null,
    new_level: Number.isFinite(obj.new_level) ? obj.new_level : null,
    rotate_to: typeof obj.rotate_to === 'string' ? obj.rotate_to.toUpperCase() : null,
  };
}

// ── public: run one mini-read ────────────────────────────────────────────────
// trigger = { pair, triggerKind:'entry'|'take_profit', level, thesisNote?, position? }
// Returns { ...decision, model, context } or throws (monitor treats a throw as "hold").
export async function runMiniRead(trigger) {
  const [asset, btc] = await Promise.all([
    tickerRead(trigger.pair),
    tickerRead('BTCUSD'),
  ]);
  const user = buildUserPrompt(trigger, asset, btc);
  const resp = await anthropic.messages.create({
    model: MODEL,
    max_tokens: 1024,
    system: SYSTEM,
    messages: [{ role: 'user', content: user }],
  });
  const text = (resp.content || [])
    .filter(b => b.type === 'text')
    .map(b => b.text)
    .join('\n');
  const decision = parseDecision(text);
  return { ...decision, model: MODEL, context: { asset, btc, prompt: user } };
}

export { tickerRead };
