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
//          holder count, sat-floor price, market cap.
//
// Failure mode: every fetch is wrapped in try/catch with a hard timeout.
// On failure the function returns null; the caller treats null as "no data
// this watch" and Capt simply doesn't reference that source. No spin, no
// stale fallback — see Option 1 honesty principle.
//
// SANITY GUARD (added): a successful fetch is NOT the same as good data. A
// feed can return 200 OK with a zero, a null, or a stale/structural value
// that isn't a real signal. Every numeric metric is now passed through sane()
// before it leaves this module — implausible values collapse to null so the
// existing "is this present?" checks in formatOnchainSection AND the dashboard
// skip them. Capt narrates validated numbers only, never a zero he'd dress up
// as "silence." See the DOG return block for the fields we suppress outright.
// =============================================================================

const HEDERA_MIRROR_BASE = 'https://mainnet-public.mirrornode.hedera.com';
const UNISAT_BASE        = 'https://open-api.unisat.io';

const DOG_RUNE_TICK = 'DOG•GO•TO•THE•MOON';

const TIMEOUT_MS = 12000;  // 12s — Mirror Node is occasionally slow under load

// =============================================================================
// SANITY GUARD
// =============================================================================
// A numeric metric is only trustworthy if it's a finite, positive number.
// Anything else (null, NaN, 0, negative) means the feed returned nothing real
// for that field this watch. Collapsing those to null lets every downstream
// consumer's existing presence check drop the field instead of narrating it.
// NOTE: use this only for metrics where zero is implausible (TPS, holders,
// price, market cap). Metrics where zero is legitimately meaningful — e.g.
// gas-used-in-window — are left untouched.
const sane = (v) => (Number.isFinite(v) && v > 0) ? v : null;

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
    // GUARD: a 0 or NaN TPS means the window math failed or the chain returned
    // nothing usable — null it so the HBAR block degrades gracefully rather
    // than Capt reading "0 TPS" as a dead chain.
    tps_avg:        sane(tpsAvg),
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
    total_supply_hbar:    sane(tinybarToHbar(data?.total_supply)),
    released_supply_hbar: sane(tinybarToHbar(data?.released_supply)),
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
// returns holders, sat-floor price, and market cap for a specific rune tick in
// a single call. We send the rune tick string and the timeType ("day1").
//
// WHAT THIS ENDPOINT IS GOOD FOR (real, fresh, runes-native — we keep these):
//   - holders            (accumulation/distribution signal)
//   - current_price_sats  (the sat-floor — DOG's value against BTC)
//   - market cap          (cap in sats/BTC + capUSD)
//
// WHAT THIS ENDPOINT IS NOT GOOD FOR (suppressed — see return block):
//   - btcVolume / amountVolume : AUCTION-house volume only. DOG trades almost
//        entirely on CEXes (Gate.io, MEXC) since Magic Eden shut down, so this
//        reads ~0 every watch. Structural, not transient — never a real signal.
//   - transactions : a CUMULATIVE lifetime count, NOT a 24h figure. It sits
//        static across runs. Surfacing it as "24h transactions" was wrong.
//   - changePrice / changePercent : come back 0 from this endpoint; not a
//        reliable 24h move.
//
// If a genuine DOG volume / on-chain activity signal is wanted later, that's a
// separate source (Hiro Runes API for on-chain transfer activity; CoinGecko for
// real cross-venue 24h volume) — not this endpoint.
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
  // Unisat returns `cap` denominated in SATOSHIS, not BTC. We divide by 1e8
  // (sats per BTC) to get BTC, and preserve raw sats for sat-native context.
  // `curPrice` is already in sats per unit. `capUSD` is already actual USD.
  // (Verified empirically: cap 89.9B sats = 899 BTC = capUSD $65.6M at ~$73k/BTC,
  //  and 899 BTC / 0.899 sats = 100B supply — all internally consistent.)
  const SATS_PER_BTC = 1e8;
  const satsToBtc = (v) => {
    const n = num(v);
    return n !== null ? n / SATS_PER_BTC : null;
  };
  return {
    tick:                  d.tick || DOG_RUNE_TICK,
    symbol:                d.symbol || null,

    // --- TRUSTWORTHY FIELDS (sanity-gated) ---
    // Real, fresh, runes-native. sane() collapses any future garbage value
    // (0 / NaN / negative) to null so consumers skip it instead of narrating it.
    holders:               sane(num(d.holders)),
    current_price_sats:    sane(num(d.curPrice)),   // sat-floor — runes-native price
    market_cap_btc:        sane(satsToBtc(d.cap)),
    market_cap_sats:       sane(num(d.cap)),
    market_cap_usd:        sane(num(d.capUSD)),

    // --- SUPPRESSED FIELDS (intentionally null) ---
    // These are structurally unreliable on the auction endpoint (see header).
    // Nulling them here means formatOnchainSection and the dashboard both skip
    // them automatically via their existing presence checks — Capt can't read,
    // and can't narrate, data that isn't a real signal. This is the fix for the
    // "clean zero sats / 24h transactions" misreads, and it holds every watch.
    transactions:          null,   // was a cumulative lifetime count, not 24h
    btc_volume_24h:        null,   // Unisat auction volume only — ~0 for DOG
    btc_volume_24h_sats:   null,
    amount_volume_24h:     null,
    change_price_24h:      null,   // unreliable 0 from this endpoint

    deploy_time:           num(d.deployTime),
  };
}

export async function fetchDogActivity(apiKey) {
  try {
    const stats = await fetchUnisatDogStats(apiKey);
    // Treat a payload with no usable core signal as a failed feed — if neither
    // holders nor sat-floor survived the sanity gate, there's nothing real to
    // report, so signal "unavailable" rather than handing Capt an empty shell.
    if (stats.holders === null && stats.current_price_sats === null) {
      return { ok: false, errors: ['Unisat returned no usable DOG signal (holders + sat-floor both failed sanity check)'] };
    }
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
