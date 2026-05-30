// =============================================================================
// server.js — Capt's Ledger dashboard
//
// Public read-only view of The Watch. The old contest-era ops control panel
// (with the Run button) is retired — manual fires now live in the !watch-fire
// Discord command. This server's job is to show, not to do.
//
// Port: 4444 (on-brand for Baby Boons / B4E)
// Path: /         — single-page dashboard from public/index.html
// API:  /api/state          live account + open positions with unrealized P&L
//       /api/equity         equity curve snapshot timeseries
//       /api/closes         recent closed trades
//       /api/decisions      recent Bridge Log runs with thesis text
//       /api/plank-walks    bankruptcy reset history
//       /healthz            simple health check
// =============================================================================

import express from 'express';
import path from 'path';
import { spawn } from 'child_process';
import { fileURLToPath } from 'url';
import { existsSync } from 'fs';
import Database from 'better-sqlite3';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// =============================================================================
// CONFIG
// =============================================================================

const PORT        = parseInt(process.env.DASHBOARD_PORT || '4444', 10);
const LEDGER_DB   = path.join(__dirname, 'data', 'ledger.db');
const PAUSE_FLAG  = path.join(__dirname, 'data', 'PAUSED.flag');
const PUBLIC_DIR  = path.join(__dirname, 'public');

const PAIRS_ALL = ['HBARUSD', 'BTCUSD', 'DOGUSD', 'SOLUSD', 'SUIUSD'];

// =============================================================================
// LEDGER CONNECTION (read-only)
// =============================================================================

let db = null;

function openLedger() {
  if (!existsSync(LEDGER_DB)) {
    console.warn(`[dashboard] Ledger DB not found at ${LEDGER_DB} — endpoints will return empty data`);
    return null;
  }
  return new Database(LEDGER_DB, { readonly: true, fileMustExist: true });
}

function getDb() {
  if (db) return db;
  db = openLedger();
  return db;
}

// =============================================================================
// CACHE
// =============================================================================
// Used for expensive operations (kraken CLI spawns). SQLite reads are fast
// enough to not need caching.

const cache = new Map();

async function cachedAsync(key, ttlMs, fn) {
  const entry = cache.get(key);
  if (entry && entry.expiresAt > Date.now()) return entry.value;
  const value = await fn();
  cache.set(key, { value, expiresAt: Date.now() + ttlMs });
  return value;
}

// =============================================================================
// KRAKEN CLI HELPER
// =============================================================================

function runKraken(args, timeoutMs = 10_000) {
  return new Promise((resolve, reject) => {
    const proc = spawn('kraken', args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let stdout = '', stderr = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.stderr.on('data', (d) => { stderr += d; });
    const timer = setTimeout(() => {
      proc.kill('SIGKILL');
      reject(new Error(`kraken ${args.join(' ')} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    proc.on('close', (code) => {
      clearTimeout(timer);
      if (code !== 0) {
        return reject(new Error(`kraken ${args.join(' ')} exited ${code}: ${stderr.trim().slice(0, 200)}`));
      }
      try { resolve(JSON.parse(stdout)); }
      catch (e) { reject(new Error(`Failed to parse kraken JSON: ${e.message}`)); }
    });
    proc.on('error', (e) => {
      clearTimeout(timer);
      reject(new Error(`Failed to spawn kraken: ${e.message}`));
    });
  });
}

function pickPair(data, pair) {
  if (data[pair]) return data[pair];
  if (pair === 'BTCUSD') return data['XXBTZUSD'] || data['XBTUSD'] || null;
  return null;
}

function symbolOf(pair) { return pair.replace('USD', ''); }

// =============================================================================
// DATA FETCHERS
// =============================================================================

async function fetchPaperStatus() {
  return cachedAsync('paper_status', 5000, async () => {
    return await runKraken(['paper', 'status', '-o', 'json']);
  });
}

async function fetchTickers(pairs) {
  if (pairs.length === 0) return {};
  const cacheKey = `tickers:${[...pairs].sort().join(',')}`;
  return cachedAsync(cacheKey, 5000, async () => {
    const data = await runKraken(['ticker', ...pairs, '-o', 'json']);
    const tickers = {};
    for (const pair of pairs) {
      const d = pickPair(data, pair);
      if (d) {
        const last = parseFloat(d.c[0]);
        const open = parseFloat(d.o);
        tickers[symbolOf(pair)] = {
          last,
          open,
          high_24h: parseFloat(d.h[1]),
          low_24h:  parseFloat(d.l[1]),
          volume_24h: parseFloat(d.v[1]),
          pct_change_24h: (open > 0) ? ((last - open) / open) * 100 : null,
        };
      }
    }
    return tickers;
  });
}

function getOpenTrades() {
  const conn = getDb();
  if (!conn) return [];
  return conn.prepare(`
    SELECT trade_id, run_id, ts_utc, pair, side, size_label, volume,
           fill_price, cost_usd, fee_usd, tier_at_entry, forced,
           invalidation_price, take_profit_price, time_stop_hours
    FROM trades
    WHERE status = 'open'
    ORDER BY ts_utc ASC
  `).all();
}

function getClosedTrades(limit) {
  const conn = getDb();
  if (!conn) return [];
  return conn.prepare(`
    SELECT trade_id, run_id, ts_utc, pair, side, size_label, volume,
           fill_price, cost_usd, fee_usd, exit_price, exit_ts_utc,
           exit_reason, pnl_usd, pnl_pct, forced
    FROM trades
    WHERE status = 'closed'
    ORDER BY exit_ts_utc DESC
    LIMIT ?
  `).all(limit);
}

function getRecentDecisions(limit) {
  const conn = getDb();
  if (!conn) return [];
  // Join runs + decisions + bridge_logs so each row has everything the UI needs.
  // Some runs may not have decisions (failed early) — LEFT JOIN tolerates that.
  return conn.prepare(`
    SELECT r.run_id, r.run_type, r.status, r.ts_utc AS run_ts_utc, r.duration_ms,
           d.action, d.size_label AS size, d.pair, d.thesis, d.confidence
    FROM runs r
    LEFT JOIN decisions d ON r.run_id = d.run_id
    ORDER BY r.run_id DESC
    LIMIT ?
  `).all(limit);
}

function getEquitySnapshots(limit) {
  const conn = getDb();
  if (!conn) return [];
  return conn.prepare(`
    SELECT ts_utc, account_value_usd, starting_balance_usd, unrealized_pnl_pct,
           open_positions_count
    FROM equity_snapshots
    ORDER BY ts_utc ASC
    LIMIT ?
  `).all(limit);
}

function getPlankWalks() {
  const conn = getDb();
  if (!conn) return [];
  try {
    return conn.prepare(`
      SELECT walk_id, ts_utc, ts_central, ending_equity_usd, days_alive,
             total_trades, biggest_winner_usd, biggest_chop_usd, reason,
             ended_at_run_id
      FROM plank_walks
      ORDER BY ts_utc DESC
    `).all();
  } catch {
    return [];  // table might not exist yet
  }
}

// Latest pair_snapshots row per pair. Each row carries Capt's structured read
// of that pair from the most recent watch in which it was reported — stance,
// confidence, the three-timeframe signal alignment, and his two short factor
// bullets. Used by /api/reasoning to drive the "Capt's Read" dashboard panel.
function getLatestPairSnapshots() {
  const conn = getDb();
  if (!conn) return [];
  try {
    return conn.prepare(`
      SELECT ps.snapshot_id, ps.run_id, ps.ts_utc, ps.pair,
             ps.stance, ps.confidence,
             ps.signal_intraday, ps.signal_swing, ps.signal_macro,
             ps.factor1, ps.factor2
      FROM pair_snapshots ps
      INNER JOIN (
        SELECT pair, MAX(ts_utc) AS max_ts
        FROM pair_snapshots
        GROUP BY pair
      ) latest ON ps.pair = latest.pair AND ps.ts_utc = latest.max_ts
      ORDER BY ps.pair
    `).all();
  } catch {
    return [];  // table might not exist yet (pre-migration installs)
  }
}

// Most recent Bridge Log full text. Powers the "From the Bridge" dashboard
// section so Capt's voice lives on the public surface, not Discord-only.
function getLatestBridgeLog() {
  const conn = getDb();
  if (!conn) return null;
  try {
    return conn.prepare(`
      SELECT bl.log_id, bl.run_id, bl.ts_utc, bl.ts_central, bl.log_text,
             r.run_type, r.status AS run_status
      FROM bridge_logs bl
      LEFT JOIN runs r ON bl.run_id = r.run_id
      ORDER BY bl.ts_utc DESC
      LIMIT 1
    `).get() || null;
  } catch {
    return null;
  }
}

// Latest on-chain snapshot row + 7-day rolling baselines for HBAR and DOG.
// Powers the "Chain Activity" dashboard section. Three queries combined so
// the dashboard can render: current values, status flags, and "vs baseline"
// deltas when enough samples have accumulated.
function getLatestOnchain() {
  const conn = getDb();
  if (!conn) return null;
  try {
    const latest = conn.prepare(`
      SELECT *
      FROM onchain_snapshots
      ORDER BY ts_utc DESC
      LIMIT 1
    `).get() || null;
    if (!latest) return null;
    const hbarBaseline = conn.prepare(`
      SELECT AVG(hbar_tps_avg)        AS avg_tps,
             AVG(hbar_total_tx)       AS avg_total_tx,
             AVG(hbar_total_gas_used) AS avg_gas,
             AVG(hbar_total_fees_hbar) AS avg_fees,
             COUNT(hbar_total_fees_hbar) AS fees_sample_count,
             COUNT(*)                 AS sample_count
      FROM onchain_snapshots
      WHERE hbar_status = 'ok'
        AND hbar_tps_avg IS NOT NULL
        AND ts_utc >= datetime('now', '-7 days')
        AND ts_utc < ?
    `).get(latest.ts_utc) || null;
    const dogBaseline = conn.prepare(`
      SELECT AVG(dog_holders)            AS avg_holders,
             AVG(dog_transactions)       AS avg_transactions,
             AVG(dog_btc_volume_24h)     AS avg_btc_volume,
             AVG(dog_current_price_sats) AS avg_price,
             COUNT(*)                    AS sample_count
      FROM onchain_snapshots
      WHERE dog_status = 'ok'
        AND dog_holders IS NOT NULL
        AND ts_utc >= datetime('now', '-7 days')
        AND ts_utc < ?
    `).get(latest.ts_utc) || null;
    return { latest, hbarBaseline, dogBaseline };
  } catch {
    return null;
  }
}

// =============================================================================
// EXPRESS APP
// =============================================================================

const app = express();
app.disable('x-powered-by');

// Static files (the dashboard SPA)
app.use(express.static(PUBLIC_DIR));

// CORS — public read-only API, allow any origin
app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Cache-Control', 'no-store');
  next();
});

// ---------------------------------------------------------------------------
// /healthz
// ---------------------------------------------------------------------------

app.get('/healthz', (req, res) => {
  res.json({ ok: true, ts: new Date().toISOString() });
});

// ---------------------------------------------------------------------------
// /api/state — live account + open positions + pause/scheduler info
// ---------------------------------------------------------------------------

app.get('/api/state', async (req, res) => {
  try {
    const openTrades = getOpenTrades();

    // Fetch current prices for pairs with open positions
    const uniquePairs = [...new Set(openTrades.map(t => t.pair))];
    let tickers = {};
    if (uniquePairs.length > 0) {
      try {
        tickers = await fetchTickers(uniquePairs);
      } catch (e) {
        console.error('[dashboard] fetchTickers failed:', e.message);
      }
    }

    // Live paper account
    let paper = null;
    try {
      paper = await fetchPaperStatus();
    } catch (e) {
      console.error('[dashboard] fetchPaperStatus failed:', e.message);
    }

    // Augment open trades with current price + unrealized P&L + time-stop remaining
    const nowMs = Date.now();
    const positions = openTrades.map(t => {
      const sym = symbolOf(t.pair);
      const ticker = tickers[sym];
      const currentPrice = ticker?.last ?? null;
      let unrealizedUsd = null, unrealizedPct = null, currentValueUsd = null;
      if (currentPrice !== null) {
        unrealizedUsd = (currentPrice - t.fill_price) * t.volume;
        unrealizedPct = ((currentPrice - t.fill_price) / t.fill_price) * 100;
        currentValueUsd = currentPrice * t.volume;
      }
      const entryMs = new Date(t.ts_utc).getTime();
      const elapsedHours = (nowMs - entryMs) / (1000 * 60 * 60);
      const hoursToTimeStop = (t.time_stop_hours || 48) - elapsedHours;

      return {
        trade_id: t.trade_id,
        run_id: t.run_id,
        pair: t.pair,
        symbol: sym,
        side: t.side,
        size_label: t.size_label,
        volume: t.volume,
        fill_price: t.fill_price,
        cost_usd: t.cost_usd,
        tier: t.tier_at_entry,
        forced: !!t.forced,
        opened_at: t.ts_utc,
        invalidation_price: t.invalidation_price,
        take_profit_price: t.take_profit_price,
        time_stop_hours: t.time_stop_hours,
        current_price: currentPrice,
        current_value_usd: currentValueUsd,
        unrealized_pnl_usd: unrealizedUsd,
        unrealized_pnl_pct: unrealizedPct,
        hours_to_time_stop: hoursToTimeStop > 0 ? hoursToTimeStop : 0,
      };
    });

    // Compute P&L from equity values directly. Kraken's `unrealized_pnl_pct`
    // field returns the value in percent form already (-0.5416 means -0.54%),
    // not as a fraction — multiplying by 100 was the long-standing display bug
    // that made the dashboard read -54.16% instead of -0.54%. Computing it
    // ourselves from current_value and starting_balance is unambiguous and
    // self-consistent regardless of what the CLI returns.
    let accountPayload = null;
    if (paper) {
      const equityDelta = paper.current_value - paper.starting_balance;
      const pnlPct = paper.starting_balance > 0
        ? (equityDelta / paper.starting_balance) * 100
        : 0;
      accountPayload = {
        current_value:    paper.current_value,
        starting_balance: paper.starting_balance,
        pnl_usd:          equityDelta,
        pnl_pct:          pnlPct,
        open_orders:      paper.open_orders,
        total_trades:     paper.total_trades,
      };
    }

    res.json({
      account: accountPayload,
      positions,
      paused: existsSync(PAUSE_FLAG),
      ts: new Date().toISOString(),
    });
  } catch (e) {
    console.error('[dashboard] /api/state error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/tickers — live ticker data for all 6 watched pairs (cached 5s)
// ---------------------------------------------------------------------------

app.get('/api/tickers', async (req, res) => {
  try {
    const tickers = await fetchTickers(PAIRS_ALL);
    const out = PAIRS_ALL.map(pair => {
      const sym = symbolOf(pair);
      const t = tickers[sym];
      return {
        pair,
        symbol: sym,
        last:           t?.last ?? null,
        open_24h:       t?.open ?? null,
        high_24h:       t?.high_24h ?? null,
        low_24h:        t?.low_24h ?? null,
        volume_24h:     t?.volume_24h ?? null,
        pct_change_24h: t?.pct_change_24h ?? null,
      };
    });
    res.json({ tickers: out, ts: new Date().toISOString() });
  } catch (e) {
    console.error('[dashboard] /api/tickers error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/equity — equity snapshots for the curve
// ---------------------------------------------------------------------------

app.get('/api/equity', (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '500', 10), 5000);
    const rows = getEquitySnapshots(limit);
    res.json({
      snapshots: rows.map(r => {
        // Compute pnl_pct from the snapshot's equity values rather than
        // trusting the persisted unrealized_pnl_pct column — see /api/state
        // for the full reasoning. Defensive against whatever kraken's CLI
        // wrote into the column at snapshot time.
        const pnlPct = r.starting_balance_usd > 0
          ? ((r.account_value_usd - r.starting_balance_usd) / r.starting_balance_usd) * 100
          : 0;
        return {
          ts: r.ts_utc,
          value: r.account_value_usd,
          starting: r.starting_balance_usd,
          pnl_pct: pnlPct,
          open_positions: r.open_positions_count,
        };
      }),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/closes — recent closed trades
// ---------------------------------------------------------------------------

app.get('/api/closes', (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 200);
    const rows = getClosedTrades(limit);
    res.json({ closes: rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/decisions — recent Bridge Log runs with thesis text
// ---------------------------------------------------------------------------

app.get('/api/decisions', (req, res) => {
  try {
    const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);
    const rows = getRecentDecisions(limit);
    res.json({ decisions: rows });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/stack — token-quantity scoreboard for HBAR + DOG
// ---------------------------------------------------------------------------
// Computes how many HBAR / DOG the current paper equity would buy vs.
// baseline (the first equity snapshot of the current session). This is
// the "stacking" metric — Capt's real goal isn't dollar PnL, it's growing
// the HBAR + DOG quantity over time.
// ---------------------------------------------------------------------------

const STACK_TARGETS = ['HBAR', 'DOG'];

function readSessionBaseline() {
  const conn = getDb();
  if (!conn) return null;
  let baselineRow;
  try {
    const plankRow = conn.prepare(`SELECT ts_utc FROM plank_walks ORDER BY ts_utc DESC LIMIT 1`).get();
    if (plankRow && plankRow.ts_utc) {
      baselineRow = conn.prepare(`
        SELECT ts_utc, account_value_usd, allocations_json
        FROM equity_snapshots
        WHERE ts_utc >= ?
        ORDER BY ts_utc ASC LIMIT 1
      `).get(plankRow.ts_utc);
    } else {
      baselineRow = conn.prepare(`
        SELECT ts_utc, account_value_usd, allocations_json
        FROM equity_snapshots
        ORDER BY ts_utc ASC LIMIT 1
      `).get();
    }
  } catch { return null; }
  if (!baselineRow) return null;
  let prices = null;
  if (baselineRow.allocations_json) {
    try {
      const parsed = JSON.parse(baselineRow.allocations_json);
      if (parsed && parsed.prices) prices = parsed.prices;
    } catch { /* ignore */ }
  }
  return { ts: baselineRow.ts_utc, equity: baselineRow.account_value_usd, prices };
}

app.get('/api/stack', async (req, res) => {
  try {
    const baseline = readSessionBaseline();
    if (!baseline || !baseline.prices) {
      return res.json({ available: false, reason: 'No baseline yet — waiting on first equity snapshot with per-pair prices.' });
    }
    const paper = await fetchPaperStatus().catch(() => null);
    if (!paper) return res.json({ available: false, reason: 'Paper status unavailable.' });
    const tickerPairs = STACK_TARGETS.map(s => s + 'USD');
    const tickers = await fetchTickers(tickerPairs).catch(() => ({}));
    const targets = {};
    for (const sym of STACK_TARGETS) {
      const currentPrice  = tickers[sym]?.last ?? null;
      const baselinePrice = baseline.prices[sym] ?? null;
      if (currentPrice === null || baselinePrice === null) {
        targets[sym] = { available: false };
        continue;
      }
      const currentEquivalent  = paper.current_value  / currentPrice;
      const baselineEquivalent = baseline.equity      / baselinePrice;
      const deltaUnits         = currentEquivalent - baselineEquivalent;
      const deltaPct           = baselineEquivalent > 0
        ? (deltaUnits / baselineEquivalent) * 100
        : 0;
      targets[sym] = {
        available: true,
        currentPrice, baselinePrice,
        currentEquivalent, baselineEquivalent,
        deltaUnits, deltaPct,
      };
    }
    res.json({
      available: true,
      baselineTs: baseline.ts,
      baselineEquity: baseline.equity,
      currentEquity: paper.current_value,
      targets,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/reasoning — per-pair structured read from the latest snapshot per pair
// ---------------------------------------------------------------------------
// Powers the dashboard's "Capt's Read" panel: stance + confidence + three
// timeframe signals (intraday / swing / macro) + Capt's two reasoning factors
// per pair. Returns { available: false, reason } until the LLM has populated
// pair_snapshots for at least one watch.
// ---------------------------------------------------------------------------

app.get('/api/reasoning', (req, res) => {
  try {
    const rows = getLatestPairSnapshots();
    if (rows.length === 0) {
      return res.json({
        available: false,
        reason:    'No reasoning snapshots yet — populates after the next watch fires.',
      });
    }
    const pairs = rows.map(r => ({
      pair:       r.pair,
      ts:         r.ts_utc,
      run_id:     r.run_id,
      stance:     r.stance,
      confidence: r.confidence,
      signals: {
        intraday: r.signal_intraday,
        swing:    r.signal_swing,
        macro:    r.signal_macro,
      },
      factors:    [r.factor1, r.factor2].filter(f => typeof f === 'string' && f.length > 0),
    }));
    res.json({
      available: true,
      pairs,
      ts:        new Date().toISOString(),
    });
  } catch (e) {
    console.error('[dashboard] /api/reasoning error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/latest-log — most recent Bridge Log full text
// ---------------------------------------------------------------------------
// Powers the "From the Bridge" dashboard section. Returns the full narration
// Capt filed at the most recent watch, so the holder-facing dashboard surfaces
// his voice directly instead of forcing visitors to Discord to see it.
// ---------------------------------------------------------------------------

app.get('/api/latest-log', (req, res) => {
  try {
    const row = getLatestBridgeLog();
    if (!row) {
      return res.json({
        available: false,
        reason:    'No Bridge Logs yet — the first one writes when the next watch fires.',
      });
    }
    res.json({
      available:  true,
      run_id:     row.run_id,
      ts:         row.ts_utc,
      ts_central: row.ts_central,
      run_type:   row.run_type,
      run_status: row.run_status,
      log_text:   row.log_text,
    });
  } catch (e) {
    console.error('[dashboard] /api/latest-log error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/onchain — on-chain context for HBAR + DOG stacking targets
// ---------------------------------------------------------------------------
// Returns the latest snapshot row plus the rolling 7-day baselines. The
// dashboard "Chain Activity" panel uses this to render HBAR and DOG cards
// with current values and "vs 7d" deltas. When a feed was down at the most
// recent watch the relevant fields come back null and the panel renders a
// "data unavailable" state for that source — no spin, no stale fallback.
// ---------------------------------------------------------------------------

app.get('/api/onchain', (req, res) => {
  try {
    const result = getLatestOnchain();
    if (!result) {
      return res.json({
        available: false,
        reason:    'No on-chain snapshots yet — populates after the next watch fires.',
      });
    }
    const { latest, hbarBaseline, dogBaseline } = result;

    // HBAR section — pulled out as a sub-object so the dashboard can render
    // it independently with its own status badge.
    const hbar = {
      status:           latest.hbar_status,
      ts:               latest.ts_utc,
      block_count:      latest.hbar_block_count,
      window_secs:      latest.hbar_window_secs,
      total_tx:         latest.hbar_total_tx,
      total_gas_used:   latest.hbar_total_gas_used,
      total_fees_hbar:  latest.hbar_total_fees_hbar,
      fees_estimated:   latest.hbar_fees_estimated,
      tps_avg:          latest.hbar_tps_avg,
      newest_block:     latest.hbar_newest_block,
      total_supply:     latest.hbar_total_supply,
      released_supply: latest.hbar_released_supply,
      price_usd:        latest.hbar_price_usd,
      baseline:         hbarBaseline && hbarBaseline.sample_count > 0 ? {
        avg_tps:        hbarBaseline.avg_tps,
        avg_total_tx:   hbarBaseline.avg_total_tx,
        avg_gas:        hbarBaseline.avg_gas,
        avg_fees:       hbarBaseline.avg_fees,
        fees_sample_count: hbarBaseline.fees_sample_count,
        sample_count:   hbarBaseline.sample_count,
        ready:          hbarBaseline.sample_count >= 6,
      } : null,
    };

    const dog = {
      status:             latest.dog_status,
      ts:                 latest.ts_utc,
      holders:            latest.dog_holders,
      transactions:       latest.dog_transactions,
      btc_volume_24h:     latest.dog_btc_volume_24h,
      // Sat-native counterpart of btc_volume_24h. Derived at API time rather
      // than stored — multiplying by 1e8 (sats/BTC) is lossless and avoids a
      // schema migration. The dashboard renders both alongside each other so
      // DOG holders see the runes-native sat figure they actually think in.
      btc_volume_24h_sats: Number.isFinite(latest.dog_btc_volume_24h)
        ? latest.dog_btc_volume_24h * 1e8
        : null,
      amount_volume_24h:  latest.dog_amount_volume_24h,
      current_price_sats: latest.dog_current_price_sats,
      change_price_24h:   latest.dog_change_price_24h,
      market_cap_btc:     latest.dog_market_cap_btc,
      market_cap_sats:    Number.isFinite(latest.dog_market_cap_btc)
        ? latest.dog_market_cap_btc * 1e8
        : null,
      market_cap_usd:     latest.dog_market_cap_usd,
      baseline:           dogBaseline && dogBaseline.sample_count > 0 ? {
        avg_holders:      dogBaseline.avg_holders,
        avg_transactions: dogBaseline.avg_transactions,
        avg_btc_volume:   dogBaseline.avg_btc_volume,
        avg_price:        dogBaseline.avg_price,
        sample_count:     dogBaseline.sample_count,
        ready:            dogBaseline.sample_count >= 6,
      } : null,
    };

    res.json({
      available: true,
      ts:        latest.ts_utc,
      run_id:    latest.run_id,
      hbar,
      dog,
    });
  } catch (e) {
    console.error('[dashboard] /api/onchain error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/plank-walks — bankruptcy reset history
// ---------------------------------------------------------------------------

app.get('/api/plank-walks', (req, res) => {
  try {
    res.json({ plank_walks: getPlankWalks() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/candles — OHLC candles via the Kraken CLI (the chart's price spine)
// ---------------------------------------------------------------------------
// Proxies `kraken ohlc <pair> --interval <n>` and reshapes to Lightweight
// Charts candle format ({ time, open, high, low, close }, time = UTC seconds).
// Cached ~60s per pair+interval. Same CLI that fills the trades feeds candles.
// ---------------------------------------------------------------------------

const CANDLE_INTERVALS = new Set([15, 60, 240, 1440]);  // 15m / 1h / 4h / 1d

async function fetchCandles(pair, interval) {
  return cachedAsync(`ohlc:${pair}:${interval}`, 60_000, async () => {
    const data = await runKraken(['ohlc', pair, '--interval', String(interval), '-o', 'json']);
    const rows = pickPair(data, pair);
    if (!Array.isArray(rows)) return [];
    // Kraken candle row: [time(s), open, high, low, close, vwap, volume, count]
    return rows.map(c => ({
      time:  Number(c[0]),
      open:  Number(c[1]),
      high:  Number(c[2]),
      low:   Number(c[3]),
      close: Number(c[4]),
    })).filter(c =>
      Number.isFinite(c.time) && Number.isFinite(c.open) &&
      Number.isFinite(c.high) && Number.isFinite(c.low) && Number.isFinite(c.close)
    );
  });
}

app.get('/api/candles', async (req, res) => {
  try {
    const pair = String(req.query.pair || '').toUpperCase();
    const interval = parseInt(req.query.interval || '240', 10);
    if (!PAIRS_ALL.includes(pair)) {
      return res.status(400).json({ error: `unknown pair "${pair}"` });
    }
    if (!CANDLE_INTERVALS.has(interval)) {
      return res.status(400).json({ error: `interval must be one of ${[...CANDLE_INTERVALS].join(', ')}` });
    }
    const candles = await fetchCandles(pair, interval);
    res.json({ pair, interval, candles, ts: new Date().toISOString() });
  } catch (e) {
    console.error('[dashboard] /api/candles error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ---------------------------------------------------------------------------
// /api/theses — watched levels (entry_watch) per pair, for the chart overlay
// ---------------------------------------------------------------------------
// The levels Capt is watching: price, direction (accumulate/trim/watch), state
// (watching/superseded/reached/resolved), and his note. Drawn as horizontal
// lines on the chart — solid when watching, faded when superseded/resolved.
// ---------------------------------------------------------------------------

function getTheses(pair, limit) {
  const conn = getDb();
  if (!conn) return [];
  const cols = `thesis_id, run_id, ts_utc, pair, kind, direction,
                level_low, level_high, note, state,
                trade_id, reached_run_id, resolved_run_id, updated_ts_utc`;
  try {
    if (pair) {
      return conn.prepare(`SELECT ${cols} FROM theses WHERE pair = ? ORDER BY ts_utc DESC LIMIT ?`).all(pair, limit);
    }
    return conn.prepare(`SELECT ${cols} FROM theses ORDER BY ts_utc DESC LIMIT ?`).all(limit);
  } catch {
    return [];
  }
}

app.get('/api/theses', (req, res) => {
  try {
    const pair = req.query.pair ? String(req.query.pair).toUpperCase() : null;
    if (pair && !PAIRS_ALL.includes(pair)) {
      return res.status(400).json({ error: `unknown pair "${pair}"` });
    }
    const limit = Math.min(parseInt(req.query.limit || '50', 10), 200);
    res.json({ theses: getTheses(pair, limit), ts: new Date().toISOString() });
  } catch (e) {
    console.error('[dashboard] /api/theses error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// =============================================================================
// START
// =============================================================================

app.listen(PORT, '0.0.0.0', () => {
  console.log('═══════════════════════════════════════════════════════════');
  console.log(`🏴‍☠️  Capt's Ledger — Dashboard online`);
  console.log(`    Listening:    http://0.0.0.0:${PORT}`);
  console.log(`    Public dir:   ${PUBLIC_DIR}`);
  console.log(`    Ledger DB:    ${LEDGER_DB} (${existsSync(LEDGER_DB) ? 'present' : 'NOT FOUND'})`);
  console.log(`    Pause flag:   ${existsSync(PAUSE_FLAG) ? '⏸ SET' : 'cleared'}`);
  console.log('═══════════════════════════════════════════════════════════');
});

// Graceful shutdown
function shutdown(signal) {
  console.log(`[dashboard] ${signal} received, shutting down`);
  if (db) {
    try { db.close(); } catch { /* ignore */ }
  }
  process.exit(0);
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
