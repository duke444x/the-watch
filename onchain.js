// =============================================================================
// ONCHAIN — On-chain context fetchers for The Watch's stacking targets.
//
// Two free public APIs feed Capt's reasoning beyond what Kraken can show him:
//
//   HBAR — Hedera Mirror Node (mainnet-public.mirrornode.hedera.com)
//          No auth required, generous rate limits, official Hedera-hosted.
//          Pulls recent block data to compute network TPS, transaction
//          throughput, and supply context.
//
//   DOG  — Unisat Open API (open-api.unisat.io)
//          Bearer token from UNISAT_API_KEY env var. Free tier covers our
//          ~8 requests/day comfortably (limit is 5 req/sec, 2000/day).
//          Pulls runes-marketplace activity for DOG•GO•TO•THE•MOON:
//          holder count, transactions, BTC volume, market cap.
//
// Failure mode: every fetch is wrapped in try/catch with a hard timeout.
// On failure the function returns null; the caller treats null as "no data
// this watch" and Capt simply doesn't reference that source. No spin, no
// stale fallback — see Option 1 honesty principle.
// =============================================================================

const HEDERA_MIRROR_BASE = 'https://mainnet-public.mirrornode.hedera.com';
const UNISAT_BASE        = 'https://open-api.unisat.io';

const DOG_RUNE_TICK = 'DOG•GO•TO•THE•MOON';

const TIMEOUT_MS = 12000;  // 12s — Mirror Node is occasionally slow under load

// =============================================================================
// HTTP HELPER — fetch with hard timeout, JSON parse, defensive error handling
// =============================================================================

async function fetchJson(url, opts = {}, timeoutMs = TIMEOUT_MS) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, {
      ...opts,
      signal: controller.signal,
      headers: {
        'Accept': 'application/json',
        ...(opts.headers || {}),
      },
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status} ${response.statusText} on ${url}`);
    }
    return await response.json();
  } finally {
    clearTimeout(timer);
  }
}

// =============================================================================
// HEDERA MIRROR NODE — HBAR network activity
// =============================================================================
// We pull the most recent 100 blocks. Each block carries a transaction count
// (`count`) and timestamps (`from`, `to`). From those we compute:
//   - total transactions in the window (sum of count)
//   - time span of the window (last ts - first ts)
//   - average TPS = total / span
//   - average transactions per block
//
// Plus network supply for context. This is enough for Capt to reason about
// network activity without imagining details we can't verify.
// =============================================================================

async function fetchHederaBlocks() {
  const url = `${HEDERA_MIRROR_BASE}/api/v1/blocks?limit=100&order=desc`;
  const data = await fetchJson(url);
  const blocks = Array.isArray(data?.blocks) ? data.blocks : [];
  if (blocks.length < 2) {
    throw new Error(`Hedera returned ${blocks.length} blocks; need ≥ 2 for window math`);
  }
  let totalTx = 0;
  let totalGasUsed = 0;
  for (const b of blocks) {
    if (typeof b.count === 'number') totalTx += b.count;
    if (typeof b.gas_used === 'number') totalGasUsed += b.gas_used;
  }
  // Hedera timestamps are strings like "1735689600.123456789" (seconds.nanos)
  const parseTs = (s) => parseFloat(String(s || '0').split('-')[0]);
  const newestTs = parseTs(blocks[0]?.timestamp?.to || blocks[0]?.timestamp?.from);
  const oldestTs = parseTs(blocks[blocks.length - 1]?.timestamp?.from);
  const windowSecs = (newestTs > 0 && oldestTs > 0 && newestTs > oldestTs)
    ? (newestTs - oldestTs)
    : null;
  const tpsAvg = (windowSecs && windowSecs > 0) ? (totalTx / windowSecs) : null;
  return {
    block_count:    blocks.length,
    window_secs:    windowSecs,
    total_tx:       totalTx,
    total_gas_used: totalGasUsed,
    tps_avg:        tpsAvg,
    newest_block:   blocks[0]?.number ?? null,
    oldest_block:   blocks[blocks.length - 1]?.number ?? null,
    newest_ts:      newestTs > 0 ? newestTs : null,
  };
}

async function fetchHederaSupply() {
  const url = `${HEDERA_MIRROR_BASE}/api/v1/network/supply`;
  const data = await fetchJson(url);
  // Mirror Node returns supply as raw tinybars (1 HBAR = 100,000,000 tinybars).
  const tinybarToHbar = (s) => {
    const n = parseFloat(String(s || '0'));
    return Number.isFinite(n) ? n / 1e8 : null;
  };
  return {
    total_supply_hbar:    tinybarToHbar(data?.total_supply),
    released_supply_hbar: tinybarToHbar(data?.released_supply),
  };
}

// Public fetcher for HBAR — runs the two sub-fetches in parallel and
// returns a composite object. Null fields where individual subfetches failed.
export async function fetchHbarActivity() {
  const [blocksResult, supplyResult] = await Promise.allSettled([
    fetchHederaBlocks(),
    fetchHederaSupply(),
  ]);
  const blocks = blocksResult.status === 'fulfilled' ? blocksResult.value : null;
  const supply = supplyResult.status === 'fulfilled' ? supplyResult.value : null;
  const errors = [];
  if (blocksResult.status === 'rejected') errors.push(`blocks: ${blocksResult.reason?.message || blocksResult.reason}`);
  if (supplyResult.status === 'rejected') errors.push(`supply: ${supplyResult.reason?.message || supplyResult.reason}`);
  // If BOTH failed we treat the whole HBAR feed as unavailable; null signals
  // the pipeline to drop the context block entirely this watch.
  if (!blocks && !supply) return { ok: false, errors };
  return {
    ok:     true,
    blocks, // null if that subfetch failed
    supply, // null if that subfetch failed
    errors, // non-empty if partial
  };
}

// =============================================================================
// UNISAT — DOG runes activity
// =============================================================================
// The marketplace endpoint /v3/market/runes/auction/runes_types_specified
// returns holders, transactions, BTC volume, price, and market cap for a
// specific rune tick in a single call. We send the rune tick string and the
// timeType ("day1" = 24h window).
// =============================================================================

async function fetchUnisatDogStats(apiKey) {
  const url = `${UNISAT_BASE}/v3/market/runes/auction/runes_types_specified`;
  const body = { timeType: 'day1', tick: DOG_RUNE_TICK };
  const headers = { 'Content-Type': 'application/json' };
  if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;
  const data = await fetchJson(url, {
    method:  'POST',
    headers,
    body:    JSON.stringify(body),
  });
  // Defensive parse — Unisat wraps responses as { code, msg, data: {...} }.
  if (data?.code !== undefined && data.code !== 0 && data.code !== 1) {
    throw new Error(`Unisat returned non-success code ${data.code}: ${data.msg || 'no message'}`);
  }
  const d = data?.data || {};
  const num = (v) => {
    const n = typeof v === 'number' ? v : parseFloat(String(v));
    return Number.isFinite(n) ? n : null;
  };
  // IMPORTANT — Unisat returns `btcVolume` and `cap` denominated in SATOSHIS,
  // not BTC. The field names are misleading. We divide by 1e8 (sats per BTC)
  // to get the actual BTC value, and ALSO preserve the raw sats for displays
  // that prefer the sat-native framing (which is canonical for runes tokens).
  // The math gives this away: a returned cap of 92,900,000,000 paired with
  // a returned capUSD of ~$69.5M only makes sense if the cap is sats — at
  // BTC=$74,692, that's 929 BTC × $74,692 = $69.4M.
  // `curPrice` is correctly already in sats per unit (rune-native pricing).
  // `capUSD` is correctly already in actual USD.
  const SATS_PER_BTC = 1e8;
  const satsToBtc = (v) => {
    const n = num(v);
    return n !== null ? n / SATS_PER_BTC : null;
  };
  return {
    tick:                  d.tick || DOG_RUNE_TICK,
    symbol:                d.symbol || null,
    holders:               num(d.holders),
    transactions:          num(d.transactions),
    btc_volume_24h:        satsToBtc(d.btcVolume),  // was wrongly labeled BTC by upstream
    btc_volume_24h_sats:   num(d.btcVolume),        // raw sats kept for sat-native displays
    amount_volume_24h:     num(d.amountVolume),
    current_price_sats:    num(d.curPrice),         // already sats/unit — runes-native
    change_price_24h:      num(d.changePrice),
    market_cap_btc:        satsToBtc(d.cap),        // was wrongly labeled BTC by upstream
    market_cap_sats:       num(d.cap),              // raw sats kept for context
    market_cap_usd:        num(d.capUSD),           // already actual USD
    deploy_time:           num(d.deployTime),
  };
}

export async function fetchDogActivity(apiKey) {
  try {
    const stats = await fetchUnisatDogStats(apiKey);
    return { ok: true, stats };
  } catch (err) {
    return { ok: false, errors: [err.message || String(err)] };
  }
}

// =============================================================================
// COMPOSITE — fetch both feeds in parallel
// =============================================================================

export async function fetchOnchainContext({ unisatApiKey } = {}) {
  const [hbarResult, dogResult] = await Promise.allSettled([
    fetchHbarActivity(),
    fetchDogActivity(unisatApiKey),
  ]);
  return {
    ts_utc: new Date().toISOString(),
    hbar:   hbarResult.status === 'fulfilled' ? hbarResult.value : { ok: false, errors: [hbarResult.reason?.message || String(hbarResult.reason)] },
    dog:    dogResult.status  === 'fulfilled' ? dogResult.value  : { ok: false, errors: [dogResult.reason?.message  || String(dogResult.reason)]  },
  };
}
