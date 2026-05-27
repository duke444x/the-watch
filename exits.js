// =============================================================================
// EXITS — Pure exit evaluation logic for open positions
//
// Takes an array of open trade rows from the ledger plus current ticker data,
// returns an array of close decisions for trades that should exit.
//
// No I/O, no side effects. Easy to unit-test, easy to reason about. The
// orchestration (executing the close, updating the ledger, posting the
// MARKER UPDATE) lives in exits-only.js.
// =============================================================================

export const EXIT_REASONS = {
  INVALIDATION: 'invalidation',
  TAKE_PROFIT:  'take_profit',
  TIME_STOP:    'time_stop',
};

/**
 * Convert a pair like "HBARUSD" to its short symbol "HBAR" (used as ticker key).
 */
function symbolOf(pair) {
  return pair.replace('USD', '');
}

/**
 * Evaluate a single open trade against current price.
 * Returns one of:
 *   { shouldExit: true, reason, currentPrice }
 *   { shouldExit: false, reason: 'hold', currentPrice }
 *   { shouldExit: false, reason: 'no_price', currentPrice: null }   // ticker missing
 */
export function evaluateTrade(trade, currentPrice, nowMs = Date.now()) {
  if (currentPrice === null || currentPrice === undefined) {
    return { shouldExit: false, reason: 'no_price', currentPrice: null };
  }

  // For paper-only buy positions: invalidation is BELOW entry, take-profit ABOVE.
  // The trade was validated at extraction time so we just compare directly.

  if (trade.invalidation_price !== null &&
      trade.invalidation_price !== undefined &&
      currentPrice <= trade.invalidation_price) {
    return { shouldExit: true, reason: EXIT_REASONS.INVALIDATION, currentPrice };
  }

  if (trade.take_profit_price !== null &&
      trade.take_profit_price !== undefined &&
      currentPrice >= trade.take_profit_price) {
    return { shouldExit: true, reason: EXIT_REASONS.TAKE_PROFIT, currentPrice };
  }

  if (trade.time_stop_hours && trade.time_stop_hours > 0) {
    const entryMs = new Date(trade.ts_utc).getTime();
    const ageHours = (nowMs - entryMs) / (1000 * 60 * 60);
    if (ageHours >= trade.time_stop_hours) {
      return { shouldExit: true, reason: EXIT_REASONS.TIME_STOP, currentPrice };
    }
  }

  return { shouldExit: false, reason: 'hold', currentPrice };
}

/**
 * Evaluate all open trades against current ticker data.
 *
 * @param {Array} openTrades   Trade rows from ledger.getOpenTrades()
 * @param {Object} tickers     Map of short symbol → ticker summary { last, ... }
 * @param {number} nowMs       Optional override for current time (testing)
 * @returns {Array}            Array of { trade, reason, currentPrice } for exits
 *                             Hold decisions are not included in the return.
 */
export function evaluatePositions(openTrades, tickers, nowMs = Date.now()) {
  const exitDecisions = [];
  for (const trade of openTrades) {
    const symbol = symbolOf(trade.pair);
    const ticker = tickers[symbol];
    const currentPrice = ticker?.last ?? null;
    const result = evaluateTrade(trade, currentPrice, nowMs);
    if (result.shouldExit) {
      exitDecisions.push({
        trade,
        reason: result.reason,
        currentPrice: result.currentPrice,
      });
    }
  }
  return exitDecisions;
}

/**
 * Convenience: format the reason as a human-readable label.
 */
export function reasonLabel(reason) {
  switch (reason) {
    case EXIT_REASONS.INVALIDATION: return 'invalidation hit';
    case EXIT_REASONS.TAKE_PROFIT:  return 'take-profit hit';
    case EXIT_REASONS.TIME_STOP:    return 'time-stop';
    default: return reason || 'unknown';
  }
}

/**
 * Compute P&L for a closed trade given entry/exit prices and volume.
 * Returns { pnlUsd, pnlPct }. Long-only for now.
 */
export function computePnl(entryPrice, exitPrice, volume, fees = 0) {
  const grossPnl = (exitPrice - entryPrice) * volume;
  const pnlUsd = grossPnl - fees;
  const pnlPct = entryPrice > 0 ? ((exitPrice - entryPrice) / entryPrice) * 100 : 0;
  return { pnlUsd, pnlPct };
}
