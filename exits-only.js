// =============================================================================
// EXITS-ONLY — Silent 6h exit-check pipeline
//
// Spawned by scheduler.js at 3 AM and 3 PM CT.
// Pure position management: NO Bridge Log narration.
//
// Pipeline:
//   1. Read all open trades from the ledger
//   2. If none, exit early (clean watch, no positions to manage)
//   3. Fetch current tickers for the unique pairs with open trades
//   4. Run pure evaluatePositions(openTrades, tickers)
//   5. For each exit decision:
//      a. Execute `kraken paper sell <pair> <volume>` to close the position
//      b. Compute P&L from entry/exit prices
//      c. Update ledger (closeTrade)
//      d. Post MARKER UPDATE embed to #capts-ledger
//   6. If any exits occurred, post a single summary to #watch-admin
//
// Flags:
//   --source <name>   ledger run_type tag (e.g. exit_check_overnight)
//   --dry-run         evaluate + log but don't execute closes (testing)
// =============================================================================

import { spawn } from 'child_process';
import dotenv from 'dotenv';
import Ledger from './ledger.js';
import { evaluatePositions, computePnl, reasonLabel } from './exits.js';
import { fetchOnchainContext } from './onchain.js';
import { postMarkerUpdate, postAdminEvent } from './webhooks.js';

dotenv.config({ quiet: true });

const PAIRS_ALL = ['HBARUSD', 'BTCUSD', 'DOGUSD', 'SAUCEUSD', 'GIBUSD', 'PACKUSD'];

// =============================================================================
// PRETTY-PRINTING (matches watch.js style)
// =============================================================================

const c = {
  reset: '\x1b[0m', bold: '\x1b[1m', dim: '\x1b[90m',
  cyan: '\x1b[36m', green: '\x1b[32m', yellow: '\x1b[33m',
  red: '\x1b[31m', blue: '\x1b[34m', magenta: '\x1b[35m',
};

function logStep(num, total, msg) { console.log(`\n${c.cyan}${c.bold}[${num}/${total}]${c.reset} ${c.bold}${msg}${c.reset}`); }
function logCommand(cmd) { console.log(`  ${c.dim}$ ${cmd}${c.reset}`); }
function logResult(msg)  { console.log(`  ${c.green}✓${c.reset} ${msg}`); }
function logAction(msg)  { console.log(`  ${c.magenta}→${c.reset} ${c.bold}${msg}${c.reset}`); }
function logDetail(msg)  { console.log(`  ${c.dim}  ${msg}${c.reset}`); }
function logFail(msg)    { console.log(`  ${c.red}✗${c.reset} ${msg}`); }
function logSkip(msg)    { console.log(`  ${c.dim}— ${msg}${c.reset}`); }
function logWarn(msg)    { console.log(`  ${c.yellow}⚠${c.reset} ${msg}`); }

// =============================================================================
// KRAKEN CLI RUNNER (same pattern as watch.js)
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

function pickPair(data, pair) {
  if (data[pair]) return data[pair];
  if (pair === 'BTCUSD') {
    return data['XXBTZUSD'] || data['XBTUSD'] || null;
  }
  return null;
}

function symbolOf(pair) { return pair.replace('USD', ''); }

// =============================================================================
// ARGS
// =============================================================================

function parseArgs() {
  const args = process.argv.slice(2);
  let source = 'exit_check';
  let dryRun = false;

  const sIdx = args.indexOf('--source');
  if (sIdx !== -1 && args[sIdx + 1]) {
    source = args[sIdx + 1];
  }
  if (args.includes('--dry-run')) {
    dryRun = true;
  }
  return { source, dryRun };
}

// =============================================================================
// MAIN
// =============================================================================

async function main() {
  const startTime = Date.now();
  const { source, dryRun } = parseArgs();

  console.log(`\n${c.bold}${c.yellow}🏴‍☠️  THE WATCH — Exit-Check pipeline${c.reset}`);
  console.log(`${c.dim}    Silent position management. No Bridge Log.${c.reset}`);
  console.log(`${c.dim}    Run source: ${source}${c.reset}`);
  if (dryRun) {
    console.log(`${c.yellow}    [DRY RUN — evaluating only, no closes executed]${c.reset}`);
  }

  // ---- Ledger init ----
  let ledger = null;
  let runId = null;
  try {
    ledger = new Ledger();
    runId = ledger.startRun(source);
    console.log(`${c.dim}    Ledger run #${runId} started${c.reset}`);
  } catch (e) {
    logWarn(`Ledger init failed (continuing without persistence): ${e.message}`);
  }

  let pipelineError = null;
  const exitsExecuted = [];

  try {
    // ----- [1/5] Pull open trades from the ledger --------------------------
    logStep(1, 5, 'Reading open positions from ledger...');
    if (!ledger) {
      logFail('No ledger available — cannot determine open positions');
      return;
    }
    const openTrades = ledger.getOpenTrades();
    if (openTrades.length === 0) {
      logResult('No open positions. Marker on deck. Nothing to evaluate.');
      console.log('');
      return;
    }
    logResult(`${openTrades.length} open position${openTrades.length === 1 ? '' : 's'} found:`);
    for (const t of openTrades) {
      const inv = t.invalidation_price !== null ? `inv $${t.invalidation_price}` : 'inv —';
      const tp  = t.take_profit_price  !== null ? `tp $${t.take_profit_price}`   : 'tp —';
      logDetail(`#${t.trade_id} ${t.pair} ${t.size_label} @ $${t.fill_price} (${inv}, ${tp}, time-stop ${t.time_stop_hours}h)`);
    }

    // ----- [2/5] Fetch current tickers for the pairs we hold ---------------
    logStep(2, 5, 'Fetching current ticker for open-position pairs...');
    const uniquePairs = [...new Set(openTrades.map(t => t.pair))];
    const tickerData = await runKraken(['ticker', ...uniquePairs, '-o', 'json']);
    const tickers = {};
    for (const pair of uniquePairs) {
      const data = pickPair(tickerData, pair);
      if (data) {
        tickers[symbolOf(pair)] = { last: parseFloat(data.c[0]) };
      }
    }
    const tickerSummary = uniquePairs
      .map(p => {
        const sym = symbolOf(p);
        const t = tickers[sym];
        return t ? `${sym} $${t.last}` : `${sym} (no data)`;
      })
      .join(', ');
    logResult(tickerSummary);

    // ----- [3/5] Run pure exit evaluation ----------------------------------
    logStep(3, 5, 'Evaluating exit triggers...');
    const exitDecisions = evaluatePositions(openTrades, tickers);
    if (exitDecisions.length === 0) {
      logResult('No exit triggers. All open positions stay on the plank.');
      console.log('');
      return;
    }
    logResult(`${exitDecisions.length} position${exitDecisions.length === 1 ? '' : 's'} flagged for exit:`);
    for (const d of exitDecisions) {
      logAction(`#${d.trade.trade_id} ${d.trade.pair} — ${reasonLabel(d.reason)} @ $${d.currentPrice}`);
    }

    // ----- [4/5] Execute closes (or dry-run skip) --------------------------
    logStep(4, 5, dryRun ? 'Dry run — skipping execution...' : 'Closing positions on the paper account...');

    for (const decision of exitDecisions) {
      const { trade, reason, currentPrice } = decision;
      try {
        if (dryRun) {
          const { pnlUsd, pnlPct } = computePnl(trade.fill_price, currentPrice, trade.volume, trade.fee_usd);
          logDetail(`[dry-run] would close #${trade.trade_id} ${trade.pair} at $${currentPrice} — projected P&L ${pnlUsd >= 0 ? '+' : ''}$${pnlUsd.toFixed(2)} (${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}%)`);
          continue;
        }

        // Execute the close on paper
        const result = await runKraken(['paper', 'sell', trade.pair, String(trade.volume), '-o', 'json']);
        const exitPrice = parseFloat(result.price);
        const sellFee = parseFloat(result.fee || 0);
        const totalFees = (trade.fee_usd || 0) + sellFee;
        const { pnlUsd, pnlPct } = computePnl(trade.fill_price, exitPrice, trade.volume, totalFees);

        logResult(`Closed #${trade.trade_id} ${trade.pair} @ $${exitPrice} — P&L ${pnlUsd >= 0 ? '+' : ''}$${pnlUsd.toFixed(2)} (${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}%)`);

        // Update the ledger
        try {
          ledger.closeTrade(trade.trade_id, exitPrice, reason, pnlUsd, pnlPct);
        } catch (e) {
          logWarn(`Ledger closeTrade failed for #${trade.trade_id}: ${e.message}`);
        }

        // Post the MARKER UPDATE
        try {
          const markerResult = await postMarkerUpdate({
            trade,
            exitPrice,
            exitReason: reason,
            pnlUsd,
            pnlPct,
            runId,
          });
          if (markerResult.skipped) {
            logSkip(`#capts-ledger skipped — ${markerResult.reason}`);
          } else if (markerResult.posted) {
            logResult(`Posted MARKER UPDATE to #capts-ledger (${markerResult.status})`);
          }
        } catch (e) {
          logWarn(`MARKER UPDATE post failed: ${e.message}`);
        }

        exitsExecuted.push({ trade, exitPrice, reason, pnlUsd, pnlPct });
      } catch (e) {
        logFail(`Failed to close #${trade.trade_id} ${trade.pair}: ${e.message}`);
        // Surface to #watch-admin so operators see close failures
        try {
          await postAdminEvent('error', 'Exit close failed', e.message, [
            { name: 'Trade',  value: `#${trade.trade_id}`,    inline: true },
            { name: 'Pair',   value: trade.pair,              inline: true },
            { name: 'Reason', value: reasonLabel(reason),     inline: true },
            { name: 'Run',    value: `#${runId}`,             inline: true },
          ], 'exits-only.js');
        } catch { /* admin post failure on close failure — already logged */ }
      }
    }

    // ----- [5/5] Optional summary to #watch-admin --------------------------
    logStep(5, 5, 'Wrap-up...');
    if (exitsExecuted.length > 0) {
      const totalPnl = exitsExecuted.reduce((s, x) => s + x.pnlUsd, 0);
      const summary = exitsExecuted
        .map(x => `${x.trade.pair}: ${x.pnlUsd >= 0 ? '+' : ''}$${x.pnlUsd.toFixed(2)} (${reasonLabel(x.reason)})`)
        .join('\n');
      try {
        await postAdminEvent(
          'info',
          `Exit-check fired ${exitsExecuted.length} close${exitsExecuted.length === 1 ? '' : 's'}`,
          summary,
          [
            { name: 'Total P&L', value: `${totalPnl >= 0 ? '+' : ''}$${totalPnl.toFixed(2)}`, inline: true },
            { name: 'Source',    value: source, inline: true },
            { name: 'Run',       value: `#${runId}`, inline: true },
          ],
          'exits-only.js',
        );
      } catch { /* swallow */ }
    }
    logResult(`Exit-check complete: ${exitsExecuted.length} close${exitsExecuted.length === 1 ? '' : 's'} executed.`);
    console.log('');
  } catch (err) {
    pipelineError = err;
    try {
      await postAdminEvent('error', 'Exit-check pipeline failed', err.message, [
        { name: 'Source', value: source || 'unknown',         inline: true },
        { name: 'Run',    value: runId ? `#${runId}` : '—',   inline: true },
      ], 'exits-only.js');
    } catch { /* tried */ }
  } finally {
    const elapsed = Date.now() - startTime;

    // Equity snapshot for the dashboard curve. Runs on every exit-check
    // regardless of which path the try block took (early-returned with no
    // positions, no exit triggers, or fired closes). Combined with watch.js's
    // snapshots this gives ~4 datapoints per day: 9 AM Bridge, 3 PM Exit,
    // 9 PM Bridge, 3 AM Exit. Best-effort — never blocks the run finalize.
    if (ledger && runId) {
      try {
        const paperStatus = await runKraken(['paper', 'status', '-o', 'json']);
        ledger.recordEquitySnapshot(runId, paperStatus);
        // NOTE: unrealized_pnl_pct from kraken is already in percentage terms
        // (e.g. -0.5416 means -0.5416%), so display it directly — do NOT
        // multiply by 100. This matches watch.js's display of the same field.
        // (Previous code multiplied by 100, showing -54.16% for a -0.54% loss.)
        logDetail(`Equity snapshot: $${paperStatus.current_value.toFixed(2)} (${paperStatus.unrealized_pnl_pct.toFixed(4)}%)`);
      } catch (e) {
        logWarn(`Equity snapshot failed: ${e.message}`);
      }
    }

    // On-chain snapshot — same cadence and best-effort pattern as the equity
    // snapshot above. Runs on every exit-check (3 AM / 3 PM CT) so the HBAR
    // and DOG baselines accumulate ~4 samples/day instead of 2, making the
    // 7-day divergence reads far more reliable (a real divergence stands out;
    // single-window noise gets smoothed). Pure data fetch — no LLM, no trading
    // decisions, no Bridge Log. Never blocks the run finalize.
    if (ledger && runId) {
      try {
        const onchain = await fetchOnchainContext({
          unisatApiKey: process.env.UNISAT_API_KEY || null,
        });
        ledger.recordOnchainSnapshot(runId, onchain);
        const hbarState = onchain.hbar?.ok ? 'ok' : 'down';
        const dogState  = onchain.dog?.ok ? 'ok' : 'down';
        logDetail(`On-chain snapshot recorded (HBAR ${hbarState}, DOG ${dogState})`);
      } catch (e) {
        logWarn(`On-chain snapshot failed: ${e.message}`);
      }
    }

    if (ledger && runId) {
      try {
        if (pipelineError) {
          ledger.failRun(runId, pipelineError.message.slice(0, 500), elapsed);
        } else {
          ledger.completeRun(runId, elapsed);
        }
      } catch (e) {
        logWarn(`Ledger finalize failed: ${e.message}`);
      }
      try { ledger.close(); } catch { /* ignore */ }
    }
  }

  if (pipelineError) throw pipelineError;
}

main().catch((err) => {
  console.error(`\n${c.red}✗ Exit-check failed:${c.reset} ${err.message}\n`);
  process.exit(1);
});
