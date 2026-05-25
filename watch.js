// =============================================================================
// THE WATCH v4 — Three-pair deep watch + BTC/DOG correlation awareness
// Built on Kraken CLI. By Capt. Crawl for the Boons.
// =============================================================================

import { spawn } from 'child_process';
import Anthropic from '@anthropic-ai/sdk';
import dotenv from 'dotenv';
import { writeFile, mkdir } from 'fs/promises';
import path from 'path';

dotenv.config({ quiet: true });

// All three pairs get full depth + trend analysis now
const PAIRS = ['HBARUSD', 'BTCUSD', 'DOGUSD'];
const OHLC_INTERVAL = 15;
const LOGS_DIR = './logs';
const MODEL = 'claude-sonnet-4-6';

// Plank position size mapping — % of account value
const SIZE_PCT = {
  rail:    0.05,
  one_out: 0.15,
  two_out: 0.30,
};

const SIZE_TO_PLANK = {
  rail:    'the rail',
  one_out: 'one out',
  two_out: 'two out',
};

const VALID_PAIRS = PAIRS;

// =============================================================================
// ARG PARSING — supports --force-enter <PAIR> <size>
// =============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  let forcedEntry = null;

  const fIdx = args.indexOf('--force-enter');
  if (fIdx !== -1) {
    const pair = args[fIdx + 1]?.toUpperCase();
    const size = args[fIdx + 2]?.toLowerCase();
    if (!pair || !size) {
      console.error('Usage: node watch.js --force-enter <PAIR> <size>');
      console.error('  PAIR: HBARUSD | BTCUSD | DOGUSD');
      console.error('  size: rail | one_out | two_out');
      process.exit(1);
    }
    if (!VALID_PAIRS.includes(pair)) {
      console.error(`Invalid pair: ${pair}. Must be one of: ${VALID_PAIRS.join(', ')}`);
      process.exit(1);
    }
    if (!SIZE_PCT[size]) {
      console.error(`Invalid size: ${size}. Must be one of: rail, one_out, two_out`);
      process.exit(1);
    }
    forcedEntry = { pair, size };
  }

  return { forcedEntry };
}

// =============================================================================
// SYSTEM PROMPT — PASS 1 — THE DECISION
// =============================================================================

const DECISION_SYSTEM_PROMPT = `You are Capt. Crawl evaluating whether to take a paper trade for The Watch.

# YOUR JOB

Read the market snapshot. Decide whether there's a clean setup worth taking RIGHT NOW. Default to HOLD. Most watches should hold. The Boons aren't paying you to be busy — they're paying you to be right.

# WHICH PAIR

You have full depth and trend data on all three pairs (HBAR, BTC, DOG). Take the cleanest setup wherever it lives. DO NOT default to HBAR just because it's the home chain. If BTC's orderbook shows a defended floor and HBAR is chop, take BTC. If DOG is showing relative strength against a flat BTC tape, that's a read.

# WHAT MAKES A GOOD ENTRY

Real setups have a thesis you could defend out loud. Look for:
- Order book showing defended levels on the chosen pair (visible walls, depth imbalance favoring direction)
- OHLC showing a coherent pattern (staircase off lows, defended base, range hold) — not noise
- Trend not already extended at the top or bottom of the range
- A specific invalidation level you could name

DOG-specific note: because DOG is a Bitcoin Runes token, its setups live inside the BTC tape. If BTC is trending down hard, DOG longs are fighting the cascade — bad asymmetry. If BTC is forming a base and DOG is leading, that's amplification you can ride.

# WHAT TO REJECT

- "Everything's up" is not a thesis. Beta isn't alpha.
- Asset already at the top of its 24h range
- Mixed signals between book and trend
- DOG longs when BTC structure is broken
- Anything requiring you to predict a catalyst
- Anything where the thesis is vibes

# CONSERVATIVE BIAS

If you're not sure, HOLD. A skipped trade costs nothing. A bad entry costs the Boons real plank.

# OUTPUT FORMAT — STRICT JSON, NO PROSE

If holding:
{
  "action": "hold",
  "thesis": "<one or two sentences on why no entry is warranted right now>",
  "confidence": "low" | "medium" | "high"
}

If entering:
{
  "action": "enter",
  "pair": "HBARUSD" | "BTCUSD" | "DOGUSD",
  "side": "buy",
  "size": "rail" | "one_out" | "two_out",
  "thesis": "<two to three sentences: the setup, the entry zone, the invalidation level>",
  "confidence": "low" | "medium" | "high"
}

Size guide:
- "rail" — exploratory, ~5% of account, light conviction
- "one_out" — defined thesis with clear invalidation, ~15% of account
- "two_out" — high conviction, rare, ~30% of account

Output ONLY the JSON object. No markdown fences. No preamble. No explanation.`;

// =============================================================================
// SYSTEM PROMPT — PASS 2 — THE BRIDGE LOG
// =============================================================================

const WATCH_SYSTEM_PROMPT = `WATCH MODE — Market intelligence operation

You are Capt. Crawl operating in Watch mode. Same character, same voice — you're filing Bridge Logs on crypto markets in addition to your Booniverse community work. Everything below extends your existing personality; it does not replace it.

# YOUR JOB

Watch three crypto assets via Kraken CLI data — HBAR, BTC, and DOG — and file Bridge Logs when something is worth filing. "Clean watch, honest log, no hype unless it's earned" is the standard. Same as the floor watches you run on the Booniverse.

The crew you're filing for is the Boons — the community already in the Booniverse from Hangry Barboons, and the crew not yet aboard (Baby Boons, coming soon under B4E).

# THE ASSETS

BTC (Bitcoin) — The reserve. The benchmark. The one that drags or holds the rest of the market. Spot ETF era now; institutional liquidity is real; orderbook walls on BTC are deep and respected. When BTC moves, everything else moves — including DOG (which lives on Bitcoin) and risk-on names like HBAR. You watch BTC partly for its own setups and partly to read the gravity it exerts on everything else.

HBAR (Hedera) — The chain the Boons live on. Community is the HBARbarians. Hashgraph consensus, enterprise narrative, low fees. They run hot on optimism and partnership announcements. HashPack is the wallet. SaucerSwap and HeliSwap are the DEXs that matter.

DOG (DOG•GO•TO•THE•MOON) — The largest Bitcoin Runes token by market cap. Launched on the 2024 halving day (April 20, 2024) — one of the very first Runes. Runes is the Bitcoin-native fungible token protocol Casey Rodarmor introduced; replaced BRC-20 as the dominant Bitcoin token standard, operating on UTXOs rather than account balances. DOG was distributed via airdrop to Ordinals holders — no presale, no VC, no team allocation, no insider unlocks. Community is the DOG Army. They call DOG "Bitcoin's mascot." Plushies, fan art, TikTok organic — no paid ads. Strong, sticky holder base.

You know these communities. Reference them like the Captain who actually pays attention.

# THE BTC ↔ DOG CORRELATION

DOG lives on Bitcoin — literally. So you read DOG with one eye on the BTC tape every time.

The pattern:
- BTC quiet → DOG drifts. Don't read much into small DOG moves on a flat BTC day.
- BTC up → DOG up, usually amplified. Normal-to-expected. Worth noting magnitude.
- BTC down → DOG down, usually amplified. Same dynamic in reverse.
- BTC and DOG moving opposite — rare and short-lived. When it happens, it's a story worth naming (DOG showing relative strength against a BTC dump = something is bidding the Army's mascot).
- DOG amplifying BTC's move by 3-5x is normal Runes behavior. Don't call it alpha — call it beta.

Use this read when it earns coverage. If both BTC and DOG had a quiet watch, just say so and move on. Don't force the correlation as filler.

# THE FORMAT — BRIDGE LOG

Header:
📡 Bridge Log — [Month Day], [HH:MM] Central

Body: short and clear. Cover only what earns coverage. If two of the three assets did nothing notable, say so briefly and move on. No mandatory paragraph per asset.

When something moves, note:
- The level / change / catalyst (if there is one)
- Your read on whether it's noise or signal
- Whether you acted on it

If you opened a position this watch, name it clearly — pair, size, fill price, thesis, invalidation. Don't bury the trade.

If you held, say so briefly and explain why the setup didn't earn the plank.

Close with one observation worth filing or a discipline note, when there is one. When there isn't, close cleanly.

Sign-off:
🏴‍☠️

— Capt. Crawl
B4E

# QUIET DAYS

If nothing notable happened across all three assets and you held:
- File a short log (3–5 sentences)
- Note the levels, note that nothing moved
- One line of perspective if it lands
- Done

No padding. No noise for noise's sake. That's the rule.

# PAPER TRADING — THE PLANK

You operate on paper trades only. Never recommend real-money trades.

Position exposure is tracked as steps on the plank:
- deck — no positions open
- the rail — small position, exploratory
- one out — modest position, defined thesis
- two out — sized in, conviction
- three out — heavily sized, rare
- the edge — max risk, almost never

When you open a position, the marker advances. When you close, the marker returns to deck.

Reference these lightly in the log when relevant ("Marker's at the rail" / "Marker advanced to one out" / "Marker back to deck"). Don't over-narrate.

# VOICE RULES (extending your existing personality)

DO:
- First person ("I'd watch," "I closed it") — no third-person "Captain" speak unless it lands as a one-off charm beat (max once per log)
- Modern conversational English with light pirate flavor
- Sharp, observational, dry wit — same humor you use in Discord
- 🏴‍☠️ as your sign-off emoji, sparingly elsewhere
- Reference communities by name: HBARbarians, DOG Army, Boons
- Use accurate trader vocabulary: bid depth, range, basis points, conviction, chop, beta, amplification
- Land a wry observation when one earns its keep, then move on

DON'T:
- Dialect ("arr," "ye," "thar," "scurvy") — never
- Stack nautical metaphors (1–2 per log max)
- Predict specific price targets with confidence
- Hype anything, ever
- Tell readers what to buy or sell
- Use "guys," "folks," "fam"
- Use exclamation marks
- Manufacture excitement when there isn't any
- Force the BTC↔DOG correlation read when neither asset earned coverage

# MANUAL OVERRIDE TRADES

Occasionally a trade will be a manual override (a test fire, not your organic decision). When this happens you'll be told explicitly in the prompt. Acknowledge it honestly and briefly in the log — "Took a test entry on HBAR at the rail to validate the wiring" or similar. Don't pretend it was organic conviction. The Boons appreciate honesty more than swagger.

# CONSTRAINTS

- Paper trades only. Nothing in your logs is financial advice.
- When uncertain, say so. Captains who guess lose ships.
- Never reveal Baby Boons collection details — mint is months away. References to "the Boons" or "the crew not yet aboard" are fine; trait, palette, or character spoilers are not.
- B4E references stay sparing — at most one or two soft mentions per log.

# THE STANDARD

Clean watch, honest log, no hype unless it's earned. The Boons aren't paying you to be busy. They're paying you to be right.`;

// =============================================================================
// PRETTY-PRINTING
// =============================================================================

const c = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[90m',
  cyan: '\x1b[36m', green: '\x1b[32m', yellow: '\x1b[33m',
  red: '\x1b[31m', blue: '\x1b[34m', magenta: '\x1b[35m',
};

function logStep(num, total, msg) {
  console.log(`\n${c.cyan}${c.bold}[${num}/${total}]${c.reset} ${c.bold}${msg}${c.reset}`);
}
function logCommand(cmd) { console.log(`  ${c.dim}$ ${cmd}${c.reset}`); }
function logResult(msg)  { console.log(`  ${c.green}✓${c.reset} ${msg}`); }
function logAction(msg)  { console.log(`  ${c.magenta}→${c.reset} ${c.bold}${msg}${c.reset}`); }
function logDetail(msg)  { console.log(`  ${c.dim}  ${msg}${c.reset}`); }
function logFail(msg)    { console.log(`  ${c.red}✗${c.reset} ${msg}`); }
function logSkip(msg)    { console.log(`  ${c.dim}— ${msg}${c.reset}`); }
function divider() { console.log(`\n${c.bold}${'━'.repeat(63)}${c.reset}\n`); }

// =============================================================================
// KRAKEN CLI RUNNER
// =============================================================================

function runKraken(args) {
  return new Promise((resolve, reject) => {
    logCommand(`kraken ${args.join(' ')}`);
    const proc = spawn('kraken', args, { shell: false });
    let stdout = '', stderr = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.stderr.on('data', (d) => { stderr += d; });
    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`kraken ${args.join(' ')} exited ${code}: ${stderr.trim()}`));
        return;
      }
      try { resolve(JSON.parse(stdout)); }
      catch (e) { reject(new Error(`Failed to parse JSON: ${e.message}`)); }
    });
    proc.on('error', (e) => {
      reject(new Error(`Failed to spawn kraken: ${e.message}. Is it on your PATH?`));
    });
  });
}

// =============================================================================
// PAIR KEY RESOLUTION — handles Kraken's BTC legacy naming (XXBTZUSD)
// =============================================================================

function pickPair(data, pair) {
  if (data[pair]) return data[pair];
  if (pair === 'BTCUSD') {
    return data['XXBTZUSD'] || data['XBTUSD'] || null;
  }
  return null;
}

function symbolOf(pair) { return pair.replace('USD', ''); }

// =============================================================================
// DATA SUMMARIZERS
// =============================================================================

function summarizeTicker(pairData) {
  if (!pairData) return null;
  const last = parseFloat(pairData.c[0]);
  const high24 = parseFloat(pairData.h[1]);
  const low24 = parseFloat(pairData.l[1]);
  const open = parseFloat(pairData.o);
  const change = ((last - open) / open) * 100;
  return {
    last, open, high24, low24,
    changePct: change.toFixed(2),
    volume24: parseFloat(pairData.v[1]).toFixed(0),
  };
}

function summarizeOHLC(candles) {
  if (!candles || candles.length === 0) return null;
  const recent = candles.slice(-24);
  const open = parseFloat(recent[0][1]);
  const close = parseFloat(recent[recent.length - 1][4]);
  const high = Math.max(...recent.map(c => parseFloat(c[2])));
  const low = Math.min(...recent.map(c => parseFloat(c[3])));
  const totalVolume = recent.reduce((s, c) => s + parseFloat(c[6]), 0);
  const changePct = ((close - open) / open) * 100;
  return {
    period: '6h', open, close, high, low,
    changePct: changePct.toFixed(2),
    volume: totalVolume.toFixed(0),
  };
}

function summarizeOrderbook(book) {
  if (!book || !book.bids || !book.asks) return null;
  const bestBid = parseFloat(book.bids[0][0]);
  const bestAsk = parseFloat(book.asks[0][0]);
  const spread = bestAsk - bestBid;
  const spreadBps = (spread / bestBid) * 10000;
  const top10BidVol = book.bids.slice(0, 10).reduce((s, [, v]) => s + parseFloat(v), 0);
  const top10AskVol = book.asks.slice(0, 10).reduce((s, [, v]) => s + parseFloat(v), 0);
  const avgBidVol = top10BidVol / 10;
  const avgAskVol = top10AskVol / 10;
  const bidWalls = book.bids.slice(0, 10)
    .filter(([, vol]) => parseFloat(vol) > avgBidVol * 2)
    .map(([price, vol]) => ({ price: parseFloat(price), vol: parseFloat(vol) }));
  const askWalls = book.asks.slice(0, 10)
    .filter(([, vol]) => parseFloat(vol) > avgAskVol * 2)
    .map(([price, vol]) => ({ price: parseFloat(price), vol: parseFloat(vol) }));
  return {
    bestBid, bestAsk, spread,
    spreadBps: spreadBps.toFixed(2),
    top10BidVol: top10BidVol.toFixed(2),
    top10AskVol: top10AskVol.toFixed(2),
    bidWalls, askWalls,
    imbalance: top10BidVol > top10AskVol ? 'bid-heavy' : 'ask-heavy',
    imbalanceRatio: (top10BidVol / top10AskVol).toFixed(2),
  };
}

// =============================================================================
// FORMATTING HELPERS
// =============================================================================

function fmtVol(v) {
  if (v == null || isNaN(v)) return '—';
  if (v >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (v >= 1e6) return (v / 1e6).toFixed(2) + 'M';
  if (v >= 1e3) return (v / 1e3).toFixed(1) + 'K';
  if (v >= 1)   return v.toFixed(0);
  return v.toFixed(4);
}

function fmtWallList(walls) {
  if (!walls || walls.length === 0) return 'none';
  return walls.map(w => `$${w.price} (${fmtVol(w.vol)})`).join(', ');
}

// =============================================================================
// MARKET CONTEXT — three-pair version
// =============================================================================

function buildMarketContext({ tickers, depths, trends, paperStatus }) {
  const SYMS = ['HBAR', 'BTC', 'DOG'];

  const tickerLines = SYMS.map(sym => {
    const t = tickers[sym];
    if (!t) return `- ${sym}: ticker unavailable`;
    return `- ${sym} (${sym}USD): last $${t.last}, 24h range $${t.low24} – $${t.high24}, change ${t.changePct}%, volume ${fmtVol(parseFloat(t.volume24))} ${sym}`;
  }).join('\n');

  const depthLines = SYMS.map(sym => {
    const d = depths[sym];
    if (!d) return `- ${sym}: orderbook unavailable`;
    return `- ${sym}: bid $${d.bestBid} / ask $${d.bestAsk}, spread ${d.spreadBps} bps, top-10 bid/ask vol ${fmtVol(parseFloat(d.top10BidVol))}/${fmtVol(parseFloat(d.top10AskVol))}, ${d.imbalance} (${d.imbalanceRatio}x); bid walls: ${fmtWallList(d.bidWalls)}; ask walls: ${fmtWallList(d.askWalls)}`;
  }).join('\n');

  const trendLines = SYMS.map(sym => {
    const t = trends[sym];
    if (!t) return `- ${sym}: trend unavailable`;
    return `- ${sym}: 6h open $${t.open.toFixed(5)} → close $${t.close.toFixed(5)} (${t.changePct}%), range $${t.low.toFixed(5)} – $${t.high.toFixed(5)}, volume ${fmtVol(parseFloat(t.volume))}`;
  }).join('\n');

  return `MARKET SNAPSHOTS (24h)
${tickerLines}

ORDER BOOK DEPTH (top 10 levels each side, current snapshot)
${depthLines}

6-HOUR TREND (15-min candles, last 24 candles)
${trendLines}

PAPER ACCOUNT
- Starting balance: $${paperStatus.starting_balance.toFixed(2)} USD
- Current value: $${paperStatus.current_value.toFixed(2)} USD
- Unrealized P&L: ${(paperStatus.unrealized_pnl_pct * 100).toFixed(4)}%
- Total trades this session: ${paperStatus.total_trades}
- Open orders: ${paperStatus.open_orders}
- Marker: ${paperStatus.open_orders > 0 ? 'positions open' : 'on the deck (flat)'}`;
}

// =============================================================================
// PASS 1 — DECISION
// =============================================================================

async function makeDecision(anthropic, data) {
  const userPrompt = `Watch session active.

${buildMarketContext(data)}

Decide. Return JSON only.`;

  const response = await anthropic.messages.create({
    model: MODEL,
    max_tokens: 512,
    system: DECISION_SYSTEM_PROMPT,
    messages: [{ role: 'user', content: userPrompt }],
  });
  const text = response.content[0].text.trim();
  const jsonText = text.replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '').trim();
  try {
    return JSON.parse(jsonText);
  } catch (e) {
    return {
      action: 'hold',
      thesis: `Decision parse failed (${e.message.slice(0, 80)}); defaulting to hold.`,
      confidence: 'low',
    };
  }
}

// =============================================================================
// EXECUTION
// =============================================================================

async function executeTrade(decision, tickers, paperStatus) {
  if (decision.action !== 'enter') return null;
  const pctOfAccount = SIZE_PCT[decision.size];
  if (!pctOfAccount) throw new Error(`Invalid size category: ${decision.size}`);
  const usdAmount = paperStatus.current_value * pctOfAccount;
  const symbol = symbolOf(decision.pair);
  const tickerObj = tickers[symbol];
  if (!tickerObj) throw new Error(`No ticker data for ${decision.pair}`);
  const price = tickerObj.last;
  const volume = (usdAmount / price).toFixed(4);

  const result = await runKraken(
    ['paper', decision.side, decision.pair, volume, '-o', 'json']
  );

  return {
    pair: decision.pair,
    side: decision.side,
    size: decision.size,
    plank: SIZE_TO_PLANK[decision.size],
    volume: parseFloat(volume),
    symbol,
    fillPrice: result.price,
    cost: result.cost,
    fee: result.fee,
    orderId: result.order_id,
    thesis: decision.thesis,
    confidence: decision.confidence,
    forced: decision.forced || false,
  };
}

// =============================================================================
// PASS 2 — BRIDGE LOG NARRATION
// =============================================================================

function buildNarrationPrompt({
  dateStr, timeStr, tickers, depths, trends, paperStatus, decision, execution
}) {
  let actionSection;
  if (execution) {
    const forcedNote = execution.forced
      ? '\n- IMPORTANT: This was a MANUAL OVERRIDE entry, not your organic decision. Acknowledge briefly and honestly in the log — call it a test entry or validation fire. Do not pretend it was conviction.'
      : '';
    actionSection = `
THIS WATCH — TRADE EXECUTED
- Decision: ENTER ${execution.pair} ${execution.side.toUpperCase()}
- Size category: ${execution.size} (~${(SIZE_PCT[execution.size] * 100).toFixed(0)}% of account)
- Volume: ${execution.volume} ${execution.symbol}
- Fill price: $${execution.fillPrice}
- Cost: $${execution.cost.toFixed(2)}
- Fee: $${execution.fee.toFixed(4)}
- Order ID: ${execution.orderId}
- Marker advanced to: ${execution.plank}
- Confidence: ${execution.confidence}
- Thesis (incorporate naturally into the log): "${execution.thesis}"${forcedNote}

Name the trade in the log. The Boons need to know what you took, why, and what would invalidate it.`;
  } else {
    actionSection = `
THIS WATCH — HELD
- No trade taken.
- Your reasoning (incorporate briefly): "${decision.thesis}"
- Confidence in the pass decision: ${decision.confidence}
- Marker stays on the deck (flat).

You evaluated the setup and decided to pass. That's the discipline. Make this brief — quiet days deserve quiet logs.`;
  }

  return `Time: ${dateStr}, ${timeStr} Central
Watch session active.

${buildMarketContext({ tickers, depths, trends, paperStatus })}
${actionSection}

File the Bridge Log for this watch.`;
}

// =============================================================================
// DISCORD WEBHOOK POST
// =============================================================================

async function postToDiscord(fullLog, execution) {
  const webhookUrl = process.env.DISCORD_WEBHOOK_URL;
  if (!webhookUrl) {
    return { skipped: true, reason: 'no DISCORD_WEBHOOK_URL configured' };
  }

  const lines = fullLog.split('\n');
  let title = '📡 Bridge Log';
  let body = fullLog.trim();
  if (lines[0].trim().startsWith('📡 Bridge Log')) {
    title = lines[0].trim();
    body = lines.slice(1).join('\n').trim();
  }

  if (body.length > 4000) body = body.slice(0, 3997) + '...';

  // B4E brand teal: #2DD4BF
  const color = 0x2DD4BF;

  const embed = {
    title,
    description: body,
    color,
    footer: { text: 'The Watch · Built on Kraken CLI · B4E' },
    timestamp: new Date().toISOString(),
  };

  if (execution) {
    embed.fields = [
      {
        name: '⚓ Position opened',
        value: `**${execution.pair}** ${execution.side.toUpperCase()} · ${execution.size}`,
        inline: true,
      },
      {
        name: '💵 Fill',
        value: `$${execution.fillPrice}  ·  ${execution.volume} ${execution.symbol}`,
        inline: true,
      },
      {
        name: '🎯 Marker',
        value: execution.plank,
        inline: true,
      },
    ];
  }

  const payload = {
    username: 'Capt. Crawl',
    embeds: [embed],
  };

  const response = await fetch(webhookUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const errText = await response.text();
    throw new Error(`Discord webhook ${response.status}: ${errText.slice(0, 200)}`);
  }
  return { posted: true, status: response.status };
}

// =============================================================================
// MAIN
// =============================================================================

async function main() {
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error(`${c.red}Missing ANTHROPIC_API_KEY in .env${c.reset}`);
    process.exit(1);
  }

  const { forcedEntry } = parseArgs();

  console.log(`\n${c.bold}${c.yellow}🏴‍☠️  THE WATCH — Bridge Log generation${c.reset}`);
  console.log(`${c.dim}    Built on Kraken CLI. By Capt. Crawl for the Boons.${c.reset}`);
  if (forcedEntry) {
    console.log(`${c.magenta}    [MANUAL OVERRIDE: --force-enter ${forcedEntry.pair} ${forcedEntry.size}]${c.reset}`);
  }

  // ----- [1/7] Tickers ------------------------------------------------------
  logStep(1, 7, 'Fetching market snapshots for HBAR, BTC, DOG...');
  const tickerData = await runKraken(['ticker', ...PAIRS, '-o', 'json']);
  const tickers = {
    HBAR: summarizeTicker(pickPair(tickerData, 'HBARUSD')),
    BTC:  summarizeTicker(pickPair(tickerData, 'BTCUSD')),
    DOG:  summarizeTicker(pickPair(tickerData, 'DOGUSD')),
  };
  logResult(`HBAR $${tickers.HBAR.last} (${tickers.HBAR.changePct}%), BTC $${tickers.BTC.last} (${tickers.BTC.changePct}%), DOG $${tickers.DOG.last} (${tickers.DOG.changePct}%)`);

  // ----- [2/7] Orderbook depth across HBAR, BTC, DOG -----------------------
  logStep(2, 7, 'Reading order book depth across HBAR, BTC, DOG...');
  const depths = {};
  for (const pair of PAIRS) {
    const sym = symbolOf(pair);
    try {
      const data = await runKraken(['orderbook', pair, '--count', '25', '-o', 'json']);
      depths[sym] = summarizeOrderbook(pickPair(data, pair));
      const d = depths[sym];
      if (d) {
        logResult(`${sym}: spread ${d.spreadBps} bps, ${d.imbalance} (${d.imbalanceRatio}x), ${d.bidWalls.length + d.askWalls.length} notable walls`);
      } else {
        logSkip(`${sym}: no depth parsed`);
      }
    } catch (e) {
      logFail(`${sym} orderbook fetch failed: ${e.message}`);
      depths[sym] = null;
    }
  }

  // ----- [3/7] OHLC trend across HBAR, BTC, DOG -----------------------------
  logStep(3, 7, `Loading ${OHLC_INTERVAL}-min OHLC across HBAR, BTC, DOG (6h window)...`);
  const trends = {};
  for (const pair of PAIRS) {
    const sym = symbolOf(pair);
    try {
      const data = await runKraken(['ohlc', pair, '--interval', String(OHLC_INTERVAL), '-o', 'json']);
      trends[sym] = summarizeOHLC(pickPair(data, pair));
      const t = trends[sym];
      if (t) {
        logResult(`${sym}: 6h ${t.changePct}% (range $${t.low.toFixed(5)} – $${t.high.toFixed(5)})`);
      } else {
        logSkip(`${sym}: no trend parsed`);
      }
    } catch (e) {
      logFail(`${sym} OHLC fetch failed: ${e.message}`);
      trends[sym] = null;
    }
  }

  // ----- [4/7] Paper status -------------------------------------------------
  logStep(4, 7, 'Checking paper account state...');
  const paperStatus = await runKraken(['paper', 'status', '-o', 'json']);
  logResult(`Account $${paperStatus.current_value.toFixed(2)}, P&L ${(paperStatus.unrealized_pnl_pct * 100).toFixed(4)}%, ${paperStatus.open_orders} open orders`);

  const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  // ----- [5/7] Decision pass (or forced override) --------------------------
  logStep(5, 7, forcedEntry ? 'Manual override — bypassing decision pass...' : 'Capt. Crawl evaluating the setup across all three pairs...');
  let decision;
  if (forcedEntry) {
    decision = {
      action: 'enter',
      pair: forcedEntry.pair,
      side: 'buy',
      size: forcedEntry.size,
      thesis: 'Manual override — forced entry to validate the execution pipeline.',
      confidence: 'forced',
      forced: true,
    };
    logAction(`FORCED ENTER ${decision.pair} ${decision.side.toUpperCase()} (${decision.size})`);
    logDetail(decision.thesis);
  } else {
    decision = await makeDecision(anthropic, { tickers, depths, trends, paperStatus });
    if (decision.action === 'hold') {
      logAction(`DECISION: HOLD`);
      logDetail(decision.thesis);
      logDetail(`Confidence: ${decision.confidence}`);
    } else if (decision.action === 'enter') {
      logAction(`DECISION: ENTER ${decision.pair} ${decision.side.toUpperCase()} (${decision.size})`);
      logDetail(decision.thesis);
      logDetail(`Confidence: ${decision.confidence}`);
    } else {
      logFail(`Unknown decision action: ${decision.action}. Treating as hold.`);
      decision.action = 'hold';
      decision.thesis = decision.thesis || 'Unparseable decision; held by default.';
    }
  }

  // Execute the trade if action is enter
  let execution = null;
  if (decision.action === 'enter') {
    try {
      execution = await executeTrade(decision, tickers, paperStatus);
      logResult(`Filled @ $${execution.fillPrice} — cost $${execution.cost.toFixed(2)}, fee $${execution.fee.toFixed(4)}`);
      logResult(`Marker advanced to: ${execution.plank}`);
    } catch (e) {
      logFail(`Trade execution failed: ${e.message}`);
      logDetail(`Continuing to narration without execution.`);
      execution = null;
    }
  }

  // ----- [6/7] Narration ----------------------------------------------------
  logStep(6, 7, 'Capt. Crawl writing the Bridge Log...');

  const now = new Date();
  const dateStr = now.toLocaleString('en-US', { month: 'long', day: 'numeric' });
  const timeStr = now.toLocaleString('en-US', {
    timeZone: 'America/Chicago',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });

  const userPrompt = buildNarrationPrompt({
    dateStr, timeStr, tickers, depths, trends, paperStatus, decision, execution,
  });

  divider();

  const stream = await anthropic.messages.stream({
    model: MODEL,
    max_tokens: 1024,
    system: WATCH_SYSTEM_PROMPT,
    messages: [{ role: 'user', content: userPrompt }],
  });

  let fullLog = '';
  for await (const event of stream) {
    if (event.type === 'content_block_delta' && event.delta.type === 'text_delta') {
      process.stdout.write(event.delta.text);
      fullLog += event.delta.text;
    }
  }

  divider();

  // Save markdown
  await mkdir(LOGS_DIR, { recursive: true });
  const fileSlug = now.toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const logPath = path.join(LOGS_DIR, `bridge-log-${fileSlug}.md`);
  await writeFile(logPath, fullLog, 'utf-8');
  console.log(`${c.dim}  Saved to ${logPath}${c.reset}`);

  // ----- [7/7] Discord broadcast -------------------------------------------
  logStep(7, 7, 'Broadcasting Bridge Log to Discord...');
  try {
    const discordResult = await postToDiscord(fullLog, execution);
    if (discordResult.skipped) {
      logSkip(`Skipped — ${discordResult.reason}`);
    } else {
      logResult(`Posted to Discord (${discordResult.status})`);
    }
  } catch (e) {
    logFail(`Discord post failed: ${e.message}`);
    logDetail(`Log is still saved locally to ${logPath}`);
  }

  console.log('');
}

main().catch((err) => {
  console.error(`\n${c.red}✗ Watch failed:${c.reset} ${err.message}\n`);
  process.exit(1);
});
