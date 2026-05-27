// =============================================================================
// WEBHOOKS — Shared Discord posting module for The Watch
//
// Three target channels, each with a purpose-specific embed shape:
//   - #bridge-log    → twice-daily Bridge Logs (postBridgeLog)
//   - #capts-ledger  → trade entries (postTradeEvent) + exits (postMarkerUpdate)
//   - #watch-admin   → errors, warnings, tier-cap rejections (postAdminEvent)
//
// All three webhook URLs are optional. If not configured, the matching post
// returns {skipped: true, reason} without throwing.
//
// Imported by:
//   - watch.js       (Bridge Logs, trade events, admin events)
//   - exits-only.js  (MARKER UPDATE on exits, admin events on failures)
// =============================================================================

// =============================================================================
// BRAND PALETTE
// =============================================================================

export const BRAND_TEAL  = 0x2DD4BF;
export const BRAND_CORAL = 0xFB7185;
export const TW4K_GOLD   = 0xF9C015;
export const REBEL_RED   = 0xE60000;
export const SKY_BLUE    = 0x61A6DD;

// =============================================================================
// WEBHOOK URL RESOLUTION (env-driven, with backward compat for old single URL)
// =============================================================================

export function bridgeLogWebhookUrl() {
  return process.env.BRIDGE_LOG_WEBHOOK_URL || process.env.DISCORD_WEBHOOK_URL || null;
}

export function ledgerWebhookUrl() {
  return process.env.LEDGER_WEBHOOK_URL || null;
}

export function watchAdminWebhookUrl() {
  return process.env.WATCH_ADMIN_WEBHOOK_URL || null;
}

// =============================================================================
// SHARED HTTP HELPER
// =============================================================================

/**
 * POST an embed to a Discord webhook URL.
 * Returns {posted: true, status} on success, {skipped: true, reason} if URL
 * is missing, or throws on HTTP failure.
 */
export async function postEmbed(webhookUrl, embed, channelLabel) {
  if (!webhookUrl) {
    return { skipped: true, reason: `${channelLabel} webhook URL not configured` };
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
    throw new Error(`Discord webhook (${channelLabel}) ${response.status}: ${errText.slice(0, 200)}`);
  }
  return { posted: true, status: response.status };
}

// =============================================================================
// FORMATTING HELPERS
// =============================================================================

function formatHeldDuration(entryTsUtc) {
  const entryMs = new Date(entryTsUtc).getTime();
  const nowMs = Date.now();
  const elapsedMs = Math.max(0, nowMs - entryMs);
  const hours = elapsedMs / (1000 * 60 * 60);
  if (hours < 1) {
    const minutes = Math.round(elapsedMs / (1000 * 60));
    return `${minutes}m`;
  }
  if (hours < 24) {
    return `${hours.toFixed(1)}h`;
  }
  const days = hours / 24;
  return `${days.toFixed(1)}d`;
}

// =============================================================================
// BRIDGE LOG → #bridge-log
// =============================================================================
// Clean storytelling embed. No trade fields. Trade events live in the ledger
// channel as their own posts now.
// =============================================================================

export async function postBridgeLog(fullLog, runId) {
  const lines = fullLog.split('\n');
  let title = '📡 Bridge Log';
  let body = fullLog.trim();
  if (lines[0].trim().startsWith('📡 Bridge Log')) {
    title = lines[0].trim();
    body = lines.slice(1).join('\n').trim();
  }
  if (body.length > 4000) body = body.slice(0, 3997) + '...';

  const embed = {
    title,
    description: body,
    color: BRAND_TEAL,
    footer: { text: `The Watch · Built on Kraken CLI · B4E${runId ? ` · Run #${runId}` : ''}` },
    timestamp: new Date().toISOString(),
  };
  return postEmbed(bridgeLogWebhookUrl(), embed, '#bridge-log');
}

// =============================================================================
// TRADE EVENT (entry) → #capts-ledger
// =============================================================================
// Standalone receipt when a position opens. Cross-referenced via Run #.
// Forced entries get gold; organic entries get coral.
// =============================================================================

const SIZE_PCT_LABELS = {
  rail:    '5%',
  one_out: '15%',
  two_out: '30%',
};

export async function postTradeEvent(execution, runId, levels = null) {
  const isForced = !!execution.forced;
  const title = isForced
    ? `⚠ Forced Entry — ${execution.pair}`
    : `⚓ Position Opened — ${execution.pair}`;

  let thesis = execution.thesis || '';
  if (thesis.length > 600) thesis = thesis.slice(0, 597) + '...';

  const sizePct = SIZE_PCT_LABELS[execution.size] || '?';
  const fields = [
    { name: 'Side',     value: execution.side.toUpperCase(),         inline: true },
    { name: 'Size',     value: `${execution.size} (${sizePct})`,     inline: true },
    { name: 'Tier',     value: execution.tier || '—',                inline: true },
    { name: 'Fill',     value: `$${execution.fillPrice}`,            inline: true },
    { name: 'Volume',   value: `${execution.volume} ${execution.symbol}`, inline: true },
    { name: 'Cost',     value: `$${execution.cost.toFixed(2)}`,      inline: true },
    { name: 'Marker',   value: execution.plank,                      inline: true },
    { name: 'Fee',      value: `$${execution.fee.toFixed(4)}`,       inline: true },
    { name: 'Order ID', value: execution.orderId || '—',             inline: true },
  ];

  // If exit levels were extracted, surface them so readers see the discipline
  if (levels && (levels.invalidation_price !== null || levels.take_profit_price !== null)) {
    const inv = levels.invalidation_price !== null ? `$${levels.invalidation_price}` : '—';
    const tp  = levels.take_profit_price  !== null ? `$${levels.take_profit_price}`  : '—';
    fields.push({ name: 'Invalidation', value: inv, inline: true });
    fields.push({ name: 'Take-Profit',  value: tp,  inline: true });
    fields.push({ name: 'Time-Stop',    value: `${levels.time_stop_hours}h`, inline: true });
  }

  const embed = {
    title,
    description: thesis,
    color: isForced ? TW4K_GOLD : BRAND_CORAL,
    fields,
    footer: { text: `Capt's Ledger${runId ? ` · Run #${runId}` : ''} · The Watch · B4E` },
    timestamp: new Date().toISOString(),
  };
  return postEmbed(ledgerWebhookUrl(), embed, '#capts-ledger');
}

// =============================================================================
// MARKER UPDATE (exit) → #capts-ledger
// =============================================================================
// Short receipt when a position closes. Color-coded by P&L direction (teal for
// winner, coral for chop). Title varies by exit reason.
// =============================================================================

const EXIT_REASON_LABELS = {
  invalidation: '⚠ Invalidation Hit',
  take_profit:  '🎯 Take-Profit Hit',
  time_stop:    '⏳ Time-Stop',
  plank_walk:   '☠ Plank Walk',
  rotation:     '↻ Rotated Out',
  manual:       '✋ Manual Close',
};

export async function postMarkerUpdate({ trade, exitPrice, exitReason, pnlUsd, pnlPct, runId }) {
  const label = EXIT_REASON_LABELS[exitReason] || `Closed (${exitReason})`;
  const winner = pnlUsd >= 0;
  const sign = winner ? '+' : '';
  const color = winner ? BRAND_TEAL : REBEL_RED;

  const description = `Position closed at **$${exitPrice}**.  P&L: **${sign}$${pnlUsd.toFixed(2)} (${sign}${pnlPct.toFixed(2)}%)**`;

  const symbol = trade.pair.replace('USD', '');

  const embed = {
    title: `${label} — ${trade.pair}`,
    description,
    color,
    fields: [
      { name: 'Entry',    value: `$${trade.fill_price}`,                  inline: true },
      { name: 'Exit',     value: `$${exitPrice}`,                         inline: true },
      { name: 'Size',     value: trade.size_label,                        inline: true },
      { name: 'Volume',   value: `${trade.volume} ${symbol}`,             inline: true },
      { name: 'Held for', value: formatHeldDuration(trade.ts_utc),        inline: true },
      { name: 'Marker',   value: 'back to the deck',                      inline: true },
    ],
    footer: { text: `Capt's Ledger${runId ? ` · Run #${runId}` : ''} · The Watch · B4E` },
    timestamp: new Date().toISOString(),
  };
  return postEmbed(ledgerWebhookUrl(), embed, '#capts-ledger');
}

// =============================================================================
// ADMIN EVENT → #watch-admin
// =============================================================================
// Errors, warnings, tier-cap rejections, anything operators need to see.
// level: 'error' | 'warn' | 'info'
// =============================================================================

export async function postAdminEvent(level, title, description, extraFields = [], sourceTag = 'watch.js') {
  const levelEmoji = level === 'error' ? '✗' : level === 'warn' ? '⚠' : 'ℹ';
  const levelColor = level === 'error' ? REBEL_RED : level === 'warn' ? TW4K_GOLD : SKY_BLUE;

  const embed = {
    title: `${levelEmoji} ${title}`,
    description: (description || '').slice(0, 2000),
    color: levelColor,
    fields: extraFields,
    footer: { text: `The Watch · ${sourceTag}` },
    timestamp: new Date().toISOString(),
  };
  return postEmbed(watchAdminWebhookUrl(), embed, '#watch-admin');
}
