// =============================================================================
// LEDGER — SQLite persistence layer for The Watch
//
// Captures every watch run, decision, trade, equity snapshot, and Bridge Log
// to /home/capt-crawl/watch/data/ledger.db. This is a passive layer — failures
// here never block the watch pipeline (every write is wrapped in try/catch on
// the caller side). Reads power the dashboard, admin commands, and future
// portfolio analytics.
//
// Design notes:
//   - better-sqlite3 (synchronous; sub-millisecond writes)
//   - WAL journal mode for concurrent read/write tolerance
//   - Prepared statements for hot paths
//   - Schema is idempotent (CREATE TABLE IF NOT EXISTS)
//   - All timestamps stored as UTC ISO 8601; Central display strings stored
//     separately for log embeds and dashboard rendering
// =============================================================================

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'fs';
import path from 'path';

const DEFAULT_DB_PATH = './data/ledger.db';

// =============================================================================
// SCHEMA
// =============================================================================

const SCHEMA_SQL = `
-- Every watch.js execution
CREATE TABLE IF NOT EXISTS runs (
  run_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc        TEXT    NOT NULL,
  ts_central    TEXT    NOT NULL,
  run_type      TEXT    NOT NULL,           -- 'organic' | 'forced' | 'exit_check' | 'reset'
  status        TEXT    NOT NULL,           -- 'in_progress' | 'success' | 'failed'
  error         TEXT,
  duration_ms   INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runs_ts ON runs(ts_utc);
CREATE INDEX IF NOT EXISTS idx_runs_type ON runs(run_type);

-- Capt's decision per run (one row per run, even when held)
CREATE TABLE IF NOT EXISTS decisions (
  decision_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id             INTEGER NOT NULL,
  ts_utc             TEXT    NOT NULL,
  action             TEXT    NOT NULL,      -- 'hold' | 'enter'
  pair               TEXT,                  -- only if enter
  side               TEXT,                  -- 'buy' (paper only)
  size_label         TEXT,                  -- 'rail' | 'one_out' | 'two_out'
  size_pct           REAL,
  thesis             TEXT    NOT NULL,
  confidence         TEXT,
  forced             INTEGER NOT NULL DEFAULT 0,
  tier_at_decision   TEXT,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_decisions_run ON decisions(run_id);
CREATE INDEX IF NOT EXISTS idx_decisions_pair ON decisions(pair);

-- Trade executions
-- Status 'open' = currently held; 'closed' = exited (filled in by exit-check
-- in a future step). On bankruptcy reset, all open trades get force-closed
-- with status 'closed' and exit_reason 'plank_walk'.
-- Exit levels are extracted from Capt's thesis at entry time and used by the
-- silent 6h exit-check to mechanically close positions when triggered.
CREATE TABLE IF NOT EXISTS trades (
  trade_id          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id            INTEGER NOT NULL,
  decision_id       INTEGER,
  ts_utc            TEXT    NOT NULL,
  pair              TEXT    NOT NULL,
  side              TEXT    NOT NULL,
  size_label        TEXT    NOT NULL,
  volume            REAL    NOT NULL,
  fill_price        REAL    NOT NULL,
  cost_usd          REAL    NOT NULL,
  fee_usd           REAL    NOT NULL,
  kraken_order_id   TEXT,
  tier_at_entry     TEXT,
  status            TEXT    NOT NULL DEFAULT 'open',  -- 'open' | 'closed'
  exit_price        REAL,
  exit_ts_utc       TEXT,
  exit_reason       TEXT,                              -- 'take_profit' | 'invalidation' | 'time_stop' | 'rotation' | 'plank_walk'
  pnl_usd           REAL,
  pnl_pct           REAL,
  forced            INTEGER NOT NULL DEFAULT 0,
  invalidation_price REAL,                             -- extracted from thesis at entry
  take_profit_price  REAL,                             -- extracted from thesis at entry (often null)
  time_stop_hours    INTEGER NOT NULL DEFAULT 48,      -- fallback exit when no level hits
  FOREIGN KEY (run_id) REFERENCES runs(run_id),
  FOREIGN KEY (decision_id) REFERENCES decisions(decision_id)
);
CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status);
CREATE INDEX IF NOT EXISTS idx_trades_pair ON trades(pair);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_utc);

-- Equity snapshot after each run
CREATE TABLE IF NOT EXISTS equity_snapshots (
  snapshot_id              INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id                   INTEGER NOT NULL UNIQUE,
  ts_utc                   TEXT    NOT NULL,
  account_value_usd        REAL    NOT NULL,
  starting_balance_usd     REAL    NOT NULL,
  unrealized_pnl_pct       REAL    NOT NULL,
  open_positions_count     INTEGER NOT NULL,
  total_trades_session     INTEGER NOT NULL,
  allocations_json         TEXT,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_equity_ts ON equity_snapshots(ts_utc);

-- Bridge Log full text per run
CREATE TABLE IF NOT EXISTS bridge_logs (
  log_id              INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id              INTEGER NOT NULL UNIQUE,
  ts_utc              TEXT    NOT NULL,
  ts_central          TEXT    NOT NULL,
  log_text            TEXT    NOT NULL,
  posted_to_discord   INTEGER NOT NULL DEFAULT 0,
  discord_status      INTEGER,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_bridge_logs_ts ON bridge_logs(ts_utc);

-- Bankruptcy / reset events ("walks the plank")
CREATE TABLE IF NOT EXISTS plank_walks (
  walk_id             INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_utc              TEXT    NOT NULL,
  ts_central          TEXT    NOT NULL,
  ending_equity_usd   REAL    NOT NULL,
  days_alive          INTEGER,
  total_trades        INTEGER,
  biggest_winner_usd  REAL,
  biggest_chop_usd    REAL,
  reason              TEXT    NOT NULL,      -- 'bankruptcy' | 'manual_admin_reset' | 'quarterly'
  ended_at_run_id     INTEGER,
  FOREIGN KEY (ended_at_run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_plank_walks_ts ON plank_walks(ts_utc);

CREATE TABLE IF NOT EXISTS theses (
  thesis_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id          INTEGER NOT NULL,
  ts_utc          TEXT    NOT NULL,
  pair            TEXT    NOT NULL,
  kind            TEXT    NOT NULL DEFAULT 'entry_watch',
  direction       TEXT,
  level_low       REAL,
  level_high      REAL,
  note            TEXT,
  state           TEXT    NOT NULL DEFAULT 'watching',
  trade_id        INTEGER,
  reached_run_id  INTEGER,
  resolved_run_id INTEGER,
  updated_ts_utc  TEXT,
  FOREIGN KEY (run_id)   REFERENCES runs(run_id),
  FOREIGN KEY (trade_id) REFERENCES trades(trade_id)
);
CREATE INDEX IF NOT EXISTS idx_theses_pair  ON theses(pair);
CREATE INDEX IF NOT EXISTS idx_theses_state ON theses(state);

-- Per-pair structured read each run (one row per pair per run, even on holds).
-- Powers the "Capt's Read" dashboard panel -- shows stance + confidence +
-- multi-timeframe signal alignment + Capt's two short reasoning factors for
-- every pair every watch, regardless of whether a trade fired. Sibling to
-- the existing decisions table (which stays trade-action-oriented).
CREATE TABLE IF NOT EXISTS pair_snapshots (
  snapshot_id     INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id          INTEGER NOT NULL,
  ts_utc          TEXT    NOT NULL,
  pair            TEXT    NOT NULL,
  stance          TEXT    NOT NULL,    -- 'stack' | 'buy' | 'hold' | 'sell' | 'rotate' | 'watch'
  confidence      TEXT,                -- 'low' | 'medium' | 'high'
  signal_intraday TEXT,                -- 'bull' | 'neutral' | 'bear'
  signal_swing    TEXT,                -- 'bull' | 'neutral' | 'bear'
  signal_macro    TEXT,                -- 'bull' | 'neutral' | 'bear'
  factor1         TEXT,
  factor2         TEXT,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_pair_snapshots_run  ON pair_snapshots(run_id);
CREATE INDEX IF NOT EXISTS idx_pair_snapshots_pair ON pair_snapshots(pair);
CREATE INDEX IF NOT EXISTS idx_pair_snapshots_ts   ON pair_snapshots(ts_utc);

-- On-chain context snapshots per run. Pulled from Hedera Mirror Node (HBAR
-- network metrics) and Unisat (DOG runes marketplace stats). Stored so the
-- dashboard "Chain Activity" panel can show current data AND so we can
-- compute deltas/baselines for Capt's divergence reads ("network busy vs
-- 7d avg," "holders +X in 24h," etc.).
-- One row per run; nullable columns reflect graceful degradation when one
-- of the upstream feeds was down for that watch.
CREATE TABLE IF NOT EXISTS onchain_snapshots (
  snapshot_id              INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id                   INTEGER NOT NULL UNIQUE,
  ts_utc                   TEXT    NOT NULL,
  -- HBAR (Hedera Mirror Node)
  hbar_status              TEXT,            -- 'ok' | 'partial' | 'failed'
  hbar_block_count         INTEGER,         -- blocks in the sample window
  hbar_window_secs         REAL,            -- window time span in seconds
  hbar_total_tx            INTEGER,         -- sum of tx counts in window
  hbar_total_gas_used      INTEGER,         -- EVM contract gas only (kept as a secondary DeFi signal)
  hbar_total_fees_hbar     REAL,            -- total network fees paid across the window, all tx types (HBAR)
  hbar_fees_estimated      INTEGER,         -- 1 if extrapolated (throughput spike exceeded the page cap), 0 if exact drain, NULL if feed down
  hbar_price_usd           REAL,            -- HBAR/USD spot at snapshot time (Kraken ticker), denominates the USD fee run-rate
  hbar_tps_avg             REAL,
  hbar_newest_block        INTEGER,
  hbar_oldest_block        INTEGER,
  hbar_total_supply        REAL,
  hbar_released_supply     REAL,
  -- DOG (Unisat runes marketplace)
  dog_status               TEXT,            -- 'ok' | 'failed'
  dog_holders              INTEGER,
  dog_transactions         INTEGER,
  dog_btc_volume_24h       REAL,
  dog_amount_volume_24h    REAL,
  dog_current_price_sats   REAL,
  dog_change_price_24h     REAL,
  dog_market_cap_btc       REAL,
  dog_market_cap_usd       REAL,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_onchain_snapshots_ts ON onchain_snapshots(ts_utc);

-- Proof-of-Reasoning attestations. One row per run (forward-only). Stores the
-- EXACT canonical bytes that were hashed (payload) so a verifier re-hashes
-- byte-identical input, the sha256, and the Hedera HCS coordinates once committed.
CREATE TABLE IF NOT EXISTS attestations (
  attestation_id   INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id           INTEGER NOT NULL UNIQUE,
  kind             TEXT    NOT NULL DEFAULT 'bridge_log_v1',
  payload          TEXT    NOT NULL,
  sha256           TEXT    NOT NULL,
  topic_id         TEXT,
  sequence_number  INTEGER,
  consensus_ts     TEXT,
  tx_id            TEXT,
  status           TEXT    NOT NULL DEFAULT 'pending',  -- 'pending' | 'submitted' | 'confirmed' | 'failed'
  attempts         INTEGER NOT NULL DEFAULT 0,
  last_error       TEXT,
  created_ts_utc   TEXT    NOT NULL,
  updated_ts_utc   TEXT,
  FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_attestations_status ON attestations(status);
CREATE INDEX IF NOT EXISTS idx_attestations_run ON attestations(run_id);
`;

// =============================================================================
// HELPERS
// =============================================================================

function nowUtc() {
  return new Date().toISOString();
}

function nowCentral() {
  const now = new Date();
  const date = now.toLocaleString('en-US', {
    timeZone: 'America/Chicago',
    month: 'long', day: 'numeric', year: 'numeric',
  });
  const time = now.toLocaleString('en-US', {
    timeZone: 'America/Chicago',
    hour: '2-digit', minute: '2-digit', hour12: false,
  });
  return `${date}, ${time} Central`;
}

// =============================================================================
// LEDGER CLASS
// =============================================================================

export default class Ledger {
  constructor(dbPath = DEFAULT_DB_PATH) {
    const dir = path.dirname(dbPath);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');
    this.db.pragma('foreign_keys = ON');
    this.initSchema();
    this.prepareStatements();
  }

  initSchema() {
    this.db.exec(SCHEMA_SQL);
    this.runMigrations();
  }

  // Idempotent column-add migrations for installs that predate a schema change.
  // CREATE TABLE IF NOT EXISTS doesn't update existing tables, so any new column
  // added to SCHEMA_SQL also needs a matching ALTER here for existing databases.
  runMigrations() {
    const tradeCols = new Set(
      this.db.prepare("PRAGMA table_info(trades)").all().map(c => c.name)
    );
    const migrations = [
      ['invalidation_price', 'ALTER TABLE trades ADD COLUMN invalidation_price REAL'],
      ['take_profit_price',  'ALTER TABLE trades ADD COLUMN take_profit_price REAL'],
      ['time_stop_hours',    'ALTER TABLE trades ADD COLUMN time_stop_hours INTEGER NOT NULL DEFAULT 48'],
      ['parent_trade_id',    'ALTER TABLE trades ADD COLUMN parent_trade_id INTEGER'],
    ];
    for (const [colName, sql] of migrations) {
      if (!tradeCols.has(colName)) {
        this.db.exec(sql);
      }
    }

    // onchain_snapshots: hbar_total_fees_hbar added when we swapped the
    // dashboard's EVM-only "gas used" metric for true all-activity network
    // fees (every tx type pays an HBAR fee). Existing DBs need the ALTER.
    const onchainCols = new Set(
      this.db.prepare("PRAGMA table_info(onchain_snapshots)").all().map(c => c.name)
    );
    const onchainMigrations = [
      ['hbar_total_fees_hbar', 'ALTER TABLE onchain_snapshots ADD COLUMN hbar_total_fees_hbar REAL'],
      ['hbar_fees_estimated',  'ALTER TABLE onchain_snapshots ADD COLUMN hbar_fees_estimated INTEGER'],
      ['hbar_price_usd',       'ALTER TABLE onchain_snapshots ADD COLUMN hbar_price_usd REAL'],
    ];
    for (const [colName, sql] of onchainMigrations) {
      if (!onchainCols.has(colName)) {
        this.db.exec(sql);
      }
    }
  }

  prepareStatements() {
    this.stmts = {
      // ---- runs ----
      insertRun: this.db.prepare(`
        INSERT INTO runs (ts_utc, ts_central, run_type, status)
        VALUES (?, ?, ?, 'in_progress')
      `),
      completeRun: this.db.prepare(`
        UPDATE runs SET status = 'success', duration_ms = ? WHERE run_id = ?
      `),
      failRun: this.db.prepare(`
        UPDATE runs SET status = 'failed', error = ?, duration_ms = ? WHERE run_id = ?
      `),

      // ---- decisions ----
      insertDecision: this.db.prepare(`
        INSERT INTO decisions
          (run_id, ts_utc, action, pair, side, size_label, size_pct,
           thesis, confidence, forced, tier_at_decision)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),

      // ---- trades ----
      insertTrade: this.db.prepare(`
        INSERT INTO trades
          (run_id, decision_id, ts_utc, pair, side, size_label, volume,
           fill_price, cost_usd, fee_usd, kraken_order_id, tier_at_entry,
           status, forced,
           invalidation_price, take_profit_price, time_stop_hours)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?)
      `),
      closeTrade: this.db.prepare(`
        UPDATE trades
        SET status = 'closed',
            exit_price = ?,
            exit_ts_utc = ?,
            exit_reason = ?,
            pnl_usd = ?,
            pnl_pct = ?
        WHERE trade_id = ?
      `),

      // ---- trim (partial close) ----
      getTradeById: this.db.prepare(`SELECT * FROM trades WHERE trade_id = ?`),
      insertTrimSlice: this.db.prepare(`
        INSERT INTO trades
          (run_id, decision_id, ts_utc, pair, side, size_label, volume,
           fill_price, cost_usd, fee_usd, tier_at_entry, status, forced,
           exit_price, exit_ts_utc, exit_reason, pnl_usd, pnl_pct,
           invalidation_price, take_profit_price, time_stop_hours, parent_trade_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      shrinkTrade: this.db.prepare(`
        UPDATE trades SET volume = ?, cost_usd = ?, fee_usd = ? WHERE trade_id = ?
      `),

      // ---- equity snapshots ----
      insertEquity: this.db.prepare(`
        INSERT INTO equity_snapshots
          (run_id, ts_utc, account_value_usd, starting_balance_usd,
           unrealized_pnl_pct, open_positions_count, total_trades_session,
           allocations_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `),

      // ---- bridge logs ----
      insertBridgeLog: this.db.prepare(`
        INSERT INTO bridge_logs (run_id, ts_utc, ts_central, log_text)
        VALUES (?, ?, ?, ?)
      `),
      markDiscordPosted: this.db.prepare(`
        UPDATE bridge_logs SET posted_to_discord = 1, discord_status = ? WHERE run_id = ?
      `),

      // ---- plank walks ----
      insertPlankWalk: this.db.prepare(`
        INSERT INTO plank_walks
          (ts_utc, ts_central, ending_equity_usd, days_alive,
           total_trades, biggest_winner_usd, biggest_chop_usd,
           reason, ended_at_run_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),

      // ---- theses (Capt's watched levels — the chart's watch layer) ----
      insertThesis: this.db.prepare(`
        INSERT INTO theses
          (run_id, ts_utc, pair, kind, direction, level_low, level_high, note, state, updated_ts_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'watching', ?)
      `),
      findOpenThesis: this.db.prepare(`
        SELECT * FROM theses
        WHERE pair = ? AND direction = ? AND state = 'watching'
        ORDER BY ts_utc DESC LIMIT 1
      `),
      touchThesis: this.db.prepare(`
        UPDATE theses SET updated_ts_utc = ?, note = COALESCE(?, note)
        WHERE thesis_id = ?
      `),
      supersedeThesis: this.db.prepare(`
        UPDATE theses SET state = 'superseded', resolved_run_id = ?, updated_ts_utc = ?
        WHERE thesis_id = ?
      `),

      // ---- reads ----
      getOpenTrades: this.db.prepare(`
        SELECT * FROM trades WHERE status = 'open' ORDER BY ts_utc ASC
      `),
      // Step 7 — portfolio-aware decisions need each open trade's original
      // thesis so Capt can remember why he opened it. LEFT JOIN tolerates
      // historical rows where decision_id is null.
      getOpenTradesWithThesis: this.db.prepare(`
        SELECT
          t.*,
          d.thesis     AS entry_thesis,
          d.confidence AS entry_confidence
        FROM trades t
        LEFT JOIN decisions d ON t.decision_id = d.decision_id
        WHERE t.status = 'open'
        ORDER BY t.ts_utc ASC
      `),
      getOpenTradesByPair: this.db.prepare(`
        SELECT * FROM trades WHERE status = 'open' AND pair = ? ORDER BY ts_utc ASC
      `),
      // armed entry levels still being watched (consumed by watch-monitor.js)
      getWatchingTheses: this.db.prepare(`
        SELECT * FROM theses WHERE state = 'watching' ORDER BY thesis_id ASC
      `),
      // retire a watched level once the monitor fills/acts on it
      resolveThesis: this.db.prepare(`
        UPDATE theses SET state = 'resolved', trade_id = ?, resolved_run_id = ?, updated_ts_utc = ?
        WHERE thesis_id = ?
      `),
      // Reckoning v2 -- record that price tagged an armed zone, decoupled from
      // any fill. Idempotent: only stamps if not already reached.
      markThesisReached: this.db.prepare(`
        UPDATE theses SET reached_run_id = ?, updated_ts_utc = ?
        WHERE thesis_id = ? AND reached_run_id IS NULL
      `),
      // Reckoning v2 -- flip stale, unreached watched levels to 'expired' so an
      // unfilled wait that never came true gets a terminal verdict.
      expireStaleTheses: this.db.prepare(`
        UPDATE theses SET state = 'expired', updated_ts_utc = ?
        WHERE state = 'watching' AND reached_run_id IS NULL AND ts_utc < ?
      `),
      getEquityCurve: this.db.prepare(`
        SELECT ts_utc, account_value_usd, unrealized_pnl_pct
        FROM equity_snapshots
        ORDER BY ts_utc ASC
        LIMIT ?
      `),
      getRecentBridgeLogs: this.db.prepare(`
        SELECT log_id, run_id, ts_utc, ts_central, log_text
        FROM bridge_logs
        ORDER BY ts_utc DESC
        LIMIT ?
      `),
      getLatestEquity: this.db.prepare(`
        SELECT * FROM equity_snapshots ORDER BY ts_utc DESC LIMIT 1
      `),
      getLifetimeStats: this.db.prepare(`
        SELECT
          COUNT(*)                                    AS total_trades,
          SUM(CASE WHEN status='closed' THEN 1 ELSE 0 END) AS closed_trades,
          SUM(CASE WHEN status='closed' AND pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN status='closed' AND pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
          MAX(pnl_usd)                                AS biggest_winner,
          MIN(pnl_usd)                                AS biggest_chop,
          COALESCE(SUM(CASE WHEN status='closed' THEN pnl_usd ELSE 0 END), 0) AS realized_pnl
        FROM trades
      `),
      countPlankWalks: this.db.prepare(`
        SELECT COUNT(*) AS n FROM plank_walks
      `),
      // Step 7 v2 — recent closed trades with their original thesis text.
      // Feeds the decision prompt so Capt can learn from his own history
      // (the most direct feedback loop he has).
      getRecentClosedTrades: this.db.prepare(`
        SELECT
          t.trade_id, t.pair, t.side, t.size_label,
          t.fill_price, t.exit_price, t.cost_usd, t.fee_usd,
          t.ts_utc, t.exit_ts_utc, t.exit_reason,
          t.pnl_usd, t.pnl_pct, t.forced,
          t.invalidation_price, t.take_profit_price, t.time_stop_hours,
          d.thesis     AS entry_thesis,
          d.confidence AS entry_confidence
        FROM trades t
        LEFT JOIN decisions d ON t.decision_id = d.decision_id
        WHERE t.status = 'closed' AND t.exit_reason != 'plank_walk'
        ORDER BY t.exit_ts_utc DESC
        LIMIT ?
      `),
      // Per-pair lifetime stats — so Capt sees his track record by pair.
      // Used to surface patterns like "you're 0-for-4 on BTC longs this session."
      getLifetimeStatsByPair: this.db.prepare(`
        SELECT
          pair,
          COUNT(*)                                    AS total,
          SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(CASE WHEN pnl_usd < 0 THEN 1 ELSE 0 END) AS losses,
          ROUND(AVG(pnl_usd), 2)                      AS avg_pnl,
          ROUND(SUM(pnl_usd), 2)                      AS net_pnl,
          MAX(pnl_usd)                                AS best,
          MIN(pnl_usd)                                AS worst
        FROM trades
        WHERE status = 'closed' AND exit_reason != 'plank_walk'
        GROUP BY pair
        ORDER BY total DESC
      `),
      // Step 7 v3 — session baseline for stack tracking. The session starts
      // either at process inception (no plank_walks) or after the most recent
      // plank_walk. The "baseline" is the first equity_snapshot at or after
      // that moment, which carries per-pair prices in allocations_json.
      getMostRecentPlankWalkTs: this.db.prepare(`
        SELECT ts_utc FROM plank_walks ORDER BY ts_utc DESC LIMIT 1
      `),
      getFirstSnapshotSince: this.db.prepare(`
        SELECT ts_utc, account_value_usd, allocations_json
        FROM equity_snapshots
        WHERE ts_utc >= ?
        ORDER BY ts_utc ASC
        LIMIT 1
      `),
      getFirstSnapshot: this.db.prepare(`
        SELECT ts_utc, account_value_usd, allocations_json
        FROM equity_snapshots
        ORDER BY ts_utc ASC
        LIMIT 1
      `),

      // ---- pair snapshots ----
      // One row per pair per run. recordPairSnapshots writes all 5 in a single
      // transaction so the dashboard never sees a partially-updated state.
      insertPairSnapshot: this.db.prepare(`
        INSERT INTO pair_snapshots
          (run_id, ts_utc, pair, stance, confidence,
           signal_intraday, signal_swing, signal_macro,
           factor1, factor2)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      // Latest snapshot per pair — INNER JOIN on max(ts_utc) per pair gives
      // us the most recent row regardless of which run it came from. Used by
      // the dashboard's /api/reasoning endpoint.
      getLatestPairSnapshots: this.db.prepare(`
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
      `),
      // Recent reads history for a single pair — last N snapshots ordered
      // newest-first. Used by the decision pipeline to feed Capt his own
      // recent reasoning so he can spot consistency vs drift in his calls.
      // The :limit binding lets the caller decide how many fires back to
      // surface (5 = ~2.5 days at twice-daily fires; small enough to keep
      // the prompt lean, deep enough to detect flip-flops).
      getRecentPairReads: this.db.prepare(`
        SELECT run_id, ts_utc, pair,
               stance, confidence,
               signal_intraday, signal_swing, signal_macro,
               factor1, factor2
        FROM pair_snapshots
        WHERE pair = ?
        ORDER BY ts_utc DESC
        LIMIT ?
      `),

      // ---- on-chain snapshots ----
      // One row per run. Tolerant of partial fills — null columns where the
      // upstream feed was down. UNIQUE(run_id) guards against double-writes
      // (each run produces exactly one on-chain snapshot).
      insertOnchainSnapshot: this.db.prepare(`
        INSERT INTO onchain_snapshots (
          run_id, ts_utc,
          hbar_status, hbar_block_count, hbar_window_secs, hbar_total_tx,
          hbar_total_gas_used, hbar_total_fees_hbar, hbar_fees_estimated, hbar_tps_avg,
          hbar_newest_block, hbar_oldest_block,
          hbar_total_supply, hbar_released_supply, hbar_price_usd,
          dog_status, dog_holders, dog_transactions, dog_btc_volume_24h,
          dog_amount_volume_24h, dog_current_price_sats, dog_change_price_24h,
          dog_market_cap_btc, dog_market_cap_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `),
      // Latest on-chain snapshot — single most recent row across all runs.
      getLatestOnchainSnapshot: this.db.prepare(`
        SELECT *
        FROM onchain_snapshots
        ORDER BY ts_utc DESC
        LIMIT 1
      `),
      // Baseline averages over the most recent ~7 days (12 snapshots/day × 7 ≈ 84,
      // but we keep a generous LIMIT and let AVG handle any gaps). Used by the
      // decision context to surface "X% above 7d baseline" reads. Only includes
      // snapshots where the relevant column is non-null.
      getOnchainHbarBaseline: this.db.prepare(`
        SELECT
          AVG(hbar_tps_avg)         AS avg_tps,
          AVG(hbar_total_tx)        AS avg_total_tx,
          AVG(hbar_total_gas_used)  AS avg_gas,
          AVG(hbar_total_fees_hbar) AS avg_fees,
          COUNT(*)                  AS sample_count
        FROM onchain_snapshots
        WHERE hbar_status = 'ok'
          AND hbar_tps_avg IS NOT NULL
          AND ts_utc >= datetime('now', '-7 days')
      `),
      getOnchainDogBaseline: this.db.prepare(`
        SELECT
          AVG(dog_holders)            AS avg_holders,
          AVG(dog_transactions)       AS avg_transactions,
          AVG(dog_btc_volume_24h)     AS avg_btc_volume,
          AVG(dog_current_price_sats) AS avg_price,
          COUNT(*)                    AS sample_count
        FROM onchain_snapshots
        WHERE dog_status = 'ok'
          AND dog_holders IS NOT NULL
          AND ts_utc >= datetime('now', '-7 days')
      `),

      // ---- attestations (Proof-of-Reasoning on Hedera) ----
      insertAttestation: this.db.prepare(`
        INSERT INTO attestations
          (run_id, kind, payload, sha256, status, attempts, created_ts_utc, updated_ts_utc)
        VALUES (?, ?, ?, ?, 'pending', 0, ?, ?)
      `),
      getAttestationByRun: this.db.prepare(`
        SELECT * FROM attestations WHERE run_id = ?
      `),
      markAttestationSubmitted: this.db.prepare(`
        UPDATE attestations
        SET status = 'submitted', topic_id = ?, sequence_number = ?, tx_id = ?,
            attempts = attempts + 1, last_error = NULL, updated_ts_utc = ?
        WHERE run_id = ?
      `),
      markAttestationConfirmed: this.db.prepare(`
        UPDATE attestations
        SET status = 'confirmed', consensus_ts = ?, updated_ts_utc = ?
        WHERE run_id = ?
      `),
      markAttestationError: this.db.prepare(`
        UPDATE attestations
        SET attempts = attempts + 1, last_error = ?, updated_ts_utc = ?
        WHERE run_id = ?
      `),
      getRetryableAttestations: this.db.prepare(`
        SELECT * FROM attestations
        WHERE status IN ('pending','submitted')
        ORDER BY attestation_id ASC
        LIMIT ?
      `),
      // per-run read-back for the canonical payload
      getRunRow: this.db.prepare(`SELECT * FROM runs WHERE run_id = ?`),
      getBridgeLogByRun: this.db.prepare(`SELECT * FROM bridge_logs WHERE run_id = ?`),
      getDecisionByRun: this.db.prepare(`
        SELECT * FROM decisions WHERE run_id = ? ORDER BY decision_id DESC LIMIT 1
      `),
      getTradesByRun: this.db.prepare(`
        SELECT * FROM trades WHERE run_id = ? ORDER BY trade_id ASC
      `),
      getThesesByRun: this.db.prepare(`
        SELECT * FROM theses WHERE run_id = ? ORDER BY thesis_id ASC
      `),
    };
  }

  // ==========================================================================
  // PUBLIC API — WRITES
  // ==========================================================================

  startRun(runType) {
    const result = this.stmts.insertRun.run(nowUtc(), nowCentral(), runType);
    return result.lastInsertRowid;
  }

  completeRun(runId, durationMs) {
    this.stmts.completeRun.run(durationMs, runId);
  }

  failRun(runId, errorMsg, durationMs = null) {
    this.stmts.failRun.run(errorMsg, durationMs, runId);
  }

  recordDecision(runId, decision, tier) {
    const result = this.stmts.insertDecision.run(
      runId,
      nowUtc(),
      decision.action,
      decision.pair || null,
      decision.side || null,
      decision.size || null,
      decision.size ? sizePctOf(decision.size) : null,
      decision.thesis || '',
      decision.confidence || null,
      decision.forced ? 1 : 0,
      tier || null,
    );
    return result.lastInsertRowid;
  }

  recordTrade(runId, decisionId, execution, levels = {}) {
    const result = this.stmts.insertTrade.run(
      runId,
      decisionId || null,
      nowUtc(),
      execution.pair,
      execution.side,
      execution.size,
      execution.volume,
      execution.fillPrice,
      execution.cost,
      execution.fee,
      execution.orderId || null,
      execution.tier || null,
      execution.forced ? 1 : 0,
      levels.invalidation_price ?? null,
      levels.take_profit_price ?? null,
      levels.time_stop_hours ?? 48,
    );
    return result.lastInsertRowid;
  }

  closeTrade(tradeId, exitPrice, exitReason, pnlUsd, pnlPct) {
    this.stmts.closeTrade.run(exitPrice, nowUtc(), exitReason, pnlUsd, pnlPct, tradeId);
  }

  // Partial close: realize a SLICE of an open lot and keep the rest running.
  // Inserts a new status='closed' row for the trimmed slice (so it scores as a
  // real closed trade and preserves the entry ts for held-duration) and shrinks
  // the parent lot's volume/cost/fee in place, both in one transaction.
  // pnlUsd/pnlPct are computed by the caller on the SLICE using the same
  // computePnl convention as a full close — mirrors closeTrade's contract.
  trimTrade(tradeId, fraction, exitPrice, exitReason, pnlUsd, pnlPct, runId, decisionId = null) {
    const f = Number(fraction);
    if (!Number.isFinite(f) || f <= 0 || f >= 1) {
      throw new Error(`trimTrade: fraction must be in (0,1), got ${fraction}`);
    }
    const parent = this.stmts.getTradeById.get(tradeId);
    if (!parent) throw new Error(`trimTrade: no trade #${tradeId}`);
    if (parent.status !== 'open') {
      throw new Error(`trimTrade: trade #${tradeId} is not open (status=${parent.status})`);
    }
    const sliceVolume = parent.volume   * f;
    const sliceCost   = parent.cost_usd * f;
    const sliceFee    = (parent.fee_usd || 0) * f;
    const remVolume   = parent.volume   - sliceVolume;
    const remCost     = parent.cost_usd - sliceCost;
    const remFee      = (parent.fee_usd || 0) - sliceFee;
    const apply = this.db.transaction(() => {
      this.stmts.insertTrimSlice.run(
        runId, decisionId, parent.ts_utc, parent.pair, parent.side,
        parent.size_label, sliceVolume, parent.fill_price, sliceCost, sliceFee,
        parent.tier_at_entry ?? null, parent.forced ?? 0,
        exitPrice, nowUtc(), exitReason, pnlUsd, pnlPct,
        parent.invalidation_price ?? null, parent.take_profit_price ?? null,
        parent.time_stop_hours ?? 48, tradeId,
      );
      this.stmts.shrinkTrade.run(remVolume, remCost, remFee, tradeId);
    });
    apply();
    return { sliceVolume, remVolume };
  }

  recordEquitySnapshot(runId, paperStatus, allocations = null) {
    this.stmts.insertEquity.run(
      runId,
      nowUtc(),
      paperStatus.current_value,
      paperStatus.starting_balance,
      paperStatus.unrealized_pnl_pct,
      paperStatus.open_orders,
      paperStatus.total_trades,
      allocations ? JSON.stringify(allocations) : null,
    );
  }

  recordBridgeLog(runId, logText) {
    this.stmts.insertBridgeLog.run(runId, nowUtc(), nowCentral(), logText);
  }

  markDiscordPosted(runId, status) {
    this.stmts.markDiscordPosted.run(status, runId);
  }

  recordPlankWalk({
    endingEquity, daysAlive, totalTrades,
    biggestWinner, biggestChop, reason, endedAtRunId,
  }) {
    this.stmts.insertPlankWalk.run(
      nowUtc(),
      nowCentral(),
      endingEquity,
      daysAlive || null,
      totalTrades || null,
      biggestWinner || null,
      biggestChop || null,
      reason,
      endedAtRunId || null,
    );
  }

  // Record/refresh Capt's watched levels for this run. Each pair_read may carry
  // an optional watch_level { price, direction, note }. A thesis PERSISTS across
  // watches: if Capt is still watching a level he already flagged (same pair +
  // direction, price within 0.5%, still 'watching'), we touch updated_ts rather
  // than spawn a duplicate. A genuinely new level inserts a fresh thesis. State
  // transitions (reached/entered/exited) are handled outside this method.
  recordTheses(runId, pairReads) {
    if (!pairReads || typeof pairReads !== 'object') return 0;
    const ts = nowUtc();
    const PRICE_TOL = 0.005;
    let touched = 0;
    const txn = this.db.transaction(() => {
      for (const [pair, read] of Object.entries(pairReads)) {
        const wl = read && read.watch_level;
        if (!wl || typeof wl !== 'object') continue;
        const price = Number(wl.price);
        if (!Number.isFinite(price) || price <= 0) continue;
        const direction = typeof wl.direction === 'string' ? wl.direction : 'watch';
        const note = typeof wl.note === 'string' ? wl.note.slice(0, 280) : null;
        const open = this.stmts.findOpenThesis.get(pair, direction);
        if (open && open.level_low && Math.abs(open.level_low - price) / price <= PRICE_TOL) {
          this.stmts.touchThesis.run(ts, note, open.thesis_id);
        } else {
          if (open) {
            this.stmts.supersedeThesis.run(runId, ts, open.thesis_id);
          }
          this.stmts.insertThesis.run(runId, ts, pair, 'entry_watch', direction, price, price, note, ts);
        }
        touched++;
      }
    });
    txn();
    return touched;
  }

  // Record one pair_snapshots row per pair, in a single transaction so the
  // dashboard never sees a half-populated read.
  //
  // pairReads shape (from the decision LLM, top-level "pair_reads" field):
  //   { "HBARUSD": {
  //       stance, confidence,
  //       signals: { intraday, swing, macro },
  //       factors: [factor1, factor2]
  //     }, ... }
  //
  // Defensive: missing pair_reads, missing fields, or non-object values
  // are silently skipped — never blocks the pipeline. The endpoint surfaces
  // "no reasoning yet" until the LLM starts populating the field.
  recordPairSnapshots(runId, pairReads) {
    if (!pairReads || typeof pairReads !== 'object') return 0;
    const ts = nowUtc();
    let inserted = 0;
    const txn = this.db.transaction(() => {
      for (const [pair, read] of Object.entries(pairReads)) {
        if (!read || typeof read !== 'object') continue;
        if (typeof read.stance !== 'string') continue;
        const signals = (read.signals && typeof read.signals === 'object') ? read.signals : {};
        const factors = Array.isArray(read.factors) ? read.factors : [];
        this.stmts.insertPairSnapshot.run(
          runId,
          ts,
          pair,
          read.stance,
          typeof read.confidence === 'string' ? read.confidence : null,
          typeof signals.intraday === 'string' ? signals.intraday : null,
          typeof signals.swing    === 'string' ? signals.swing    : null,
          typeof signals.macro    === 'string' ? signals.macro    : null,
          typeof factors[0] === 'string' ? factors[0] : null,
          typeof factors[1] === 'string' ? factors[1] : null,
        );
        inserted++;
      }
    });
    txn();
    return inserted;
  }

  // ==========================================================================
  // PUBLIC API — ATTESTATIONS (Proof-of-Reasoning anchoring on Hedera)
  // ==========================================================================

  // Assemble the canonical "read" for a run: the Bridge Log text PLUS the
  // structured levels the scorecard runs on (decision + trades + watched
  // theses). Returns null when the run has no Bridge Log yet (nothing to
  // anchor). The exact JSON bytes built from this are what get hashed + stored.
  getRunForAttestation(runId) {
    const bridge = this.stmts.getBridgeLogByRun.get(runId);
    if (!bridge || !bridge.log_text) return null;
    const run      = this.stmts.getRunRow.get(runId);
    const decision = this.stmts.getDecisionByRun.get(runId) || null;
    const trades   = this.stmts.getTradesByRun.all(runId) || [];
    const theses   = this.stmts.getThesesByRun.all(runId) || [];

    return {
      v: 1,
      kind: 'bridge_log_v1',
      run_id: runId,
      ts_utc: bridge.ts_utc || (run ? run.ts_utc : null),
      bridge_log: bridge.log_text,
      decision: decision ? {
        action:     decision.action,
        pair:       decision.pair,
        side:       decision.side,
        size_label: decision.size_label,
        size_pct:   decision.size_pct,
        thesis:     decision.thesis,
        confidence: decision.confidence,
        forced:     decision.forced,
      } : null,
      trades: trades.map(t => ({
        pair:               t.pair,
        side:               t.side,
        size_label:         t.size_label,
        volume:             t.volume,
        fill_price:         t.fill_price,
        invalidation_price: t.invalidation_price,
        take_profit_price:  t.take_profit_price,
        time_stop_hours:    t.time_stop_hours,
      })),
      theses: theses.map(th => ({
        pair:       th.pair,
        kind:       th.kind,
        direction:  th.direction,
        level_low:  th.level_low,
        level_high: th.level_high,
        note:       th.note,
      })),
    };
  }

  insertAttestation(runId, kind, payload, sha256) {
    const ts = nowUtc();
    this.stmts.insertAttestation.run(runId, kind, payload, sha256, ts, ts);
  }

  getAttestationByRun(runId) {
    return this.stmts.getAttestationByRun.get(runId) || null;
  }

  markAttestationSubmitted(runId, topicId, sequenceNumber, txId) {
    this.stmts.markAttestationSubmitted.run(topicId, sequenceNumber, txId || null, nowUtc(), runId);
  }

  markAttestationConfirmed(runId, consensusTs) {
    this.stmts.markAttestationConfirmed.run(consensusTs, nowUtc(), runId);
  }

  markAttestationError(runId, errMsg) {
    this.stmts.markAttestationError.run(String(errMsg).slice(0, 500), nowUtc(), runId);
  }

  getRetryableAttestations(limit = 10) {
    return this.stmts.getRetryableAttestations.all(limit);
  }

  // ==========================================================================
  // PUBLIC API — READS (used by dashboard, admin commands, future analytics)
  // ==========================================================================

  getOpenTrades() {
    return this.stmts.getOpenTrades.all();
  }

  // Step 7 — open trades enriched with their original entry thesis.
  // Used by watch.js portfolio-aware decisions; the LLM needs to see
  // why each position was opened to know whether the thesis still holds.
  getOpenTradesWithThesis() {
    return this.stmts.getOpenTradesWithThesis.all();
  }

  getOpenTradesByPair(pair) {
    return this.stmts.getOpenTradesByPair.all(pair);
  }

  // Every thesis still armed (state='watching') — the levels the watcher polls.
  getWatchingTheses() {
    return this.stmts.getWatchingTheses.all();
  }

  // Retire a watched level after the monitor acts on it (stops re-firing).
  resolveThesis(thesisId, tradeId, runId) {
    this.stmts.resolveThesis.run(tradeId || null, runId || null, nowUtc(), thesisId);
  }

  // Reckoning v2 -- stamp the run at which price first tagged this thesis's zone.
  // Idempotent (no-op if already reached). Returns true if it stamped this call.
  markThesisReached(thesisId, runId) {
    const info = this.stmts.markThesisReached.run(runId || null, nowUtc(), thesisId);
    return info.changes > 0;
  }

  // Reckoning v2 -- expire watched levels older than cutoffIso that were never
  // reached (the predicted move never came). Returns how many were expired.
  expireStaleTheses(cutoffIso) {
    const info = this.stmts.expireStaleTheses.run(nowUtc(), cutoffIso);
    return info.changes;
  }

  getEquityCurve(limit = 500) {
    return this.stmts.getEquityCurve.all(limit);
  }

  getRecentBridgeLogs(limit = 5) {
    return this.stmts.getRecentBridgeLogs.all(limit);
  }

  getLatestEquity() {
    return this.stmts.getLatestEquity.get();
  }

  getLifetimeStats() {
    const row = this.stmts.getLifetimeStats.get();
    const planks = this.stmts.countPlankWalks.get();
    return { ...row, plank_walks: planks?.n || 0 };
  }

  // Step 7 v2 — recent closes with their original thesis. Feeds the decision
  // prompt so Capt has a direct feedback loop on what's worked and what hasn't.
  getRecentClosedTrades(limit = 10) {
    return this.stmts.getRecentClosedTrades.all(limit);
  }

  // Per-pair lifetime breakdown of wins/losses. Used to surface patterns
  // ("you're 0-for-4 on BTC longs this session") in the decision prompt.
  getLifetimeStatsByPair() {
    return this.stmts.getLifetimeStatsByPair.all();
  }

  // Step 7 v3 — session baseline for stack tracking. Returns the first
  // equity_snapshot since the most recent plank_walk (or the first ever
  // if no plank walks). Parses the allocations_json into a usable shape:
  //   { ts, equity, prices: { HBAR: 0.0883, BTC: 76657, ... } }
  // Returns null if no snapshots yet (fresh session, first fire).
  getSessionBaseline() {
    let row;
    try {
      const plankRow = this.stmts.getMostRecentPlankWalkTs.get();
      if (plankRow && plankRow.ts_utc) {
        row = this.stmts.getFirstSnapshotSince.get(plankRow.ts_utc);
      } else {
        row = this.stmts.getFirstSnapshot.get();
      }
    } catch {
      return null;
    }
    if (!row) return null;
    let prices = null;
    if (row.allocations_json) {
      try {
        const parsed = JSON.parse(row.allocations_json);
        // Stored as { prices: { HBAR: 0.0883, ... } } — defensive against
        // legacy snapshots that wrote a different shape into the column.
        if (parsed && typeof parsed === 'object' && parsed.prices) {
          prices = parsed.prices;
        }
      } catch { /* malformed — treat as no baseline */ }
    }
    return {
      ts:     row.ts_utc,
      equity: row.account_value_usd,
      prices,  // may be null for legacy snapshots without per-pair prices
    };
  }

  // Per-pair structured read from each watch — latest row per pair.
  // Powers the dashboard's "Capt's Read" panel via /api/reasoning.
  getLatestPairSnapshots() {
    return this.stmts.getLatestPairSnapshots.all();
  }

  // Recent reads history grouped by pair. Returns an object keyed by pair
  // symbol, value is an array of the last `limit` reads (newest first).
  // This feeds back into Capt's decision context so he can see his OWN
  // recent reasoning per pair — surfacing consistency (good) vs drift
  // (warning sign worth examining).
  //
  // Implemented as one query per pair rather than a single grouped query
  // because SQLite's window-function pagination on ORDER BY + LIMIT per
  // partition is awkward. With 5 pairs and limit=5, this is 5 small index
  // scans — sub-millisecond total, well under any pipeline budget.
  getRecentPairReadsByPair(pairs, limit = 5) {
    const result = {};
    for (const pair of pairs) {
      try {
        result[pair] = this.stmts.getRecentPairReads.all(pair, limit) || [];
      } catch {
        result[pair] = [];
      }
    }
    return result;
  }

  // ==========================================================================
  // ON-CHAIN SNAPSHOTS — Hedera Mirror Node + Unisat runes data per run.
  // Tolerant of partial fills: HBAR or DOG can be null independently when
  // the upstream feed was unreachable for that watch.
  // ==========================================================================

  recordOnchainSnapshot(runId, payload) {
    if (!payload || typeof payload !== 'object') return null;
    const hbar = payload.hbar;
    const dog  = payload.dog;

    // HBAR fields — flattened from the composite { blocks, supply, fees, ok, errors }.
    const hbarStatus = hbar?.ok
      ? (hbar.blocks && hbar.supply ? 'ok' : 'partial')
      : 'failed';
    const blocks = hbar?.blocks || null;
    const supply = hbar?.supply || null;
    const fees   = hbar?.fees   || null;   // { total_fees_hbar, tx_sampled, estimated }
    const price  = hbar?.price  || null;   // { hbar_usd }

    // DOG fields — flattened from { stats, ok, errors }.
    const dogStatus = dog?.ok ? 'ok' : 'failed';
    const dogStats  = dog?.stats || null;

    const result = this.stmts.insertOnchainSnapshot.run(
      runId,
      nowUtc(),
      hbarStatus,
      blocks?.block_count    ?? null,
      blocks?.window_secs    ?? null,
      blocks?.total_tx       ?? null,
      blocks?.total_gas_used ?? null,
      fees?.total_fees_hbar  ?? null,
      fees ? (fees.estimated ? 1 : 0) : null,
      blocks?.tps_avg        ?? null,
      blocks?.newest_block   ?? null,
      blocks?.oldest_block   ?? null,
      supply?.total_supply_hbar    ?? null,
      supply?.released_supply_hbar ?? null,
      price?.hbar_usd              ?? null,
      dogStatus,
      dogStats?.holders             ?? null,
      dogStats?.transactions        ?? null,
      dogStats?.btc_volume_24h      ?? null,
      dogStats?.amount_volume_24h   ?? null,
      dogStats?.current_price_sats  ?? null,
      dogStats?.change_price_24h    ?? null,
      dogStats?.market_cap_btc      ?? null,
      dogStats?.market_cap_usd      ?? null,
    );
    return result.lastInsertRowid;
  }

  getLatestOnchainSnapshot() {
    return this.stmts.getLatestOnchainSnapshot.get() || null;
  }

  getOnchainHbarBaseline() {
    return this.stmts.getOnchainHbarBaseline.get() || null;
  }

  getOnchainDogBaseline() {
    return this.stmts.getOnchainDogBaseline.get() || null;
  }

  // ==========================================================================
  // LIFECYCLE
  // ==========================================================================

  close() {
    this.db.close();
  }
}

// =============================================================================
// HELPERS (kept inside the module so watch.js doesn't need to import them)
// =============================================================================

const SIZE_PCT_MAP = {
  rail:    0.05,
  one_out: 0.15,
  two_out: 0.30,
};

function sizePctOf(sizeLabel) {
  return SIZE_PCT_MAP[sizeLabel] || null;
}
