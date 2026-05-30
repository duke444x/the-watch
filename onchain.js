// =============================================================================
// ONCHAIN — On-chain context fetchers for The Watch's stacking targets.
//
// Two free public APIs feed Capt's reasoning beyond what Kraken can show him:
//
//   HBAR — Hedera Mirror Node (mainnet-public.mirrornode.hedera.com)
//          No auth required, generous rate limits, official Hedera-hosted.
//          Pulls recent block data to compute network TPS, transaction
//          throughput, total network fees paid (HBAR), and supply context.
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
const KRAKEN_TICKER_BASE = 'https://api.kraken.com';

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
// We also return the window's raw consensus-timestamp bounds (oldest_ts_str /
// newest_ts_str) so fetchHederaFees can scope its transaction sweep to exactly
// the same window the rest of the HBAR metrics describe.
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
  // Raw seconds.nanos strings for the fee window query — passed verbatim to the
  // Mirror Node `timestamp=gte:/lte:` filter so we keep full nanosecond bounds
  // (parseFloat would truncate the nanos and drift the window edge).
  const newestTsStr = blocks[0]?.timestamp?.to || blocks[0]?.timestamp?.from || null;
  const oldestTsStr = blocks[blocks.length - 1]?.timestamp?.from || null;
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
    newest_ts_str:  newestTsStr,
    oldest_ts_str:  oldestTsStr,
  };
}

// =============================================================================
// HEDERA FEES — total network fees paid across the block window (HBAR)
// =============================================================================
// Unlike gas (EVM-contract-only, and aggregated at the block level), there is
// NO block-level fee total on the Mirror Node. Fees live per-transaction as
// `charged_tx_fee` (tinybars). So the true all-activity fee number means
// summing that field across every transaction in the window.
//
// CHILD-TX NUANCE: inner / scheduled / child transactions report
// charged_tx_fee = 0 — the fee is charged to the PARENT record. So a straight
// sum over every record returned is correct: children contribute 0, parents
// carry the real fee, nothing is double-counted.
//
// THROUGHPUT GUARD: Hedera can spike into the thousands of TPS, so a ~3-min
// window can hold far more transactions than the block `count` field implies.
// We cap paging at MAX_PAGES (bounded cost every watch, regardless of load).
// If we hit the cap before draining the window, we extrapolate the unseen
// older slice by TIME coverage — sum_seen / (covered_span / window_span) —
// assuming roughly uniform fee density, and flag the result `estimated`. Using
// the transactions' own timestamps (not block counts) keeps the numerator and
// denominator on the same population, so the estimate stays internally honest.
//
// Supplementary by contract: this runs AFTER blocks and a failure returns null
// without ever sinking the HBAR read.
// =============================================================================

async function fetchHederaFees(startTs, endTs) {
  // Cap sized to fully DRAIN the window at normal mainnet throughput (~14k tx
  // at ~70 TPS) with margin, so the common case is an EXACT sum. On a genuine
  // spike the window can hold far more than any cap could drain (1000 TPS ≈
  // 200k tx) — but the cap isn't a failure point: by the time we hit it we've
  // sampled ~20k tx, which pins the fee-per-tx average down tight, so the
  // time-coverage extrapolation below is an ACCURATE rate×window estimate (not
  // the noisy small-sample kind), honestly flagged via `estimated`.
  const MAX_PAGES = 200;
  const PAGE_LIMIT = 100;          // Mirror Node max page size
  const TINYBAR_PER_HBAR = 1e8;

  if (!startTs || !endTs) return null;

  const startNum = parseFloat(String(startTs));
  const endNum   = parseFloat(String(endTs));
  const windowSpan = (Number.isFinite(startNum) && Number.isFinite(endNum) && endNum > startNum)
    ? (endNum - startNum)
    : null;

  let url = `${HEDERA_MIRROR_BASE}/api/v1/transactions`
    + `?timestamp=gte:${startTs}&timestamp=lte:${endTs}`
    + `&limit=${PAGE_LIMIT}&order=desc`;

  let totalTinybar = 0;
  let counted      = 0;
  let pages        = 0;
  let drained      = false;
  let lastSeenTs   = endNum;       // oldest ts seen so far; walks backward

  while (url && pages < MAX_PAGES) {
    let data;
    try {
      data = await fetchJson(url);
    } catch (err) {
      // A flaky page mid-sweep shouldn't nuke the whole metric: stop here and
      // extrapolate from what we have (flagged estimated below). If it fails on
      // the very first page, totalTinybar stays 0 → sane() nulls it cleanly.
      break;
    }
    const txns = Array.isArray(data?.transactions) ? data.transactions : [];
    for (const t of txns) {
      const fee = Number(t.charged_tx_fee);
      if (Number.isFinite(fee) && fee > 0) totalTinybar += fee;
      counted++;  // count every record (incl. 0-fee children) — keeps the
                  // time-coverage extrapolation on a consistent population
      const ts = parseFloat(String(t.consensus_timestamp || '0'));
      if (Number.isFinite(ts) && ts > 0) lastSeenTs = ts;
    }
    pages++;
    const next = data?.links?.next;
    if (next) {
      // links.next is a root-relative path (e.g. "/api/v1/transactions?...")
      url = next.startsWith('http') ? next : `${HEDERA_MIRROR_BASE}${next}`;
    } else {
      url = null;
      drained = true;
    }
  }

  let totalHbar = totalTinybar / TINYBAR_PER_HBAR;
  let estimated = false;

  // Hit the page cap with window still undrained: scale up by the fraction of
  // the window's time span we actually covered (order=desc means we covered
  // [lastSeenTs, endTs]).
  if (!drained && windowSpan && lastSeenTs > startNum) {
    const coveredFraction = (endNum - lastSeenTs) / windowSpan;
    if (coveredFraction > 0 && coveredFraction < 1) {
      totalHbar = totalHbar / coveredFraction;
      estimated = true;
    }
  }

  return {
    total_fees_hbar: sane(totalHbar),
    tx_sampled:      counted,
    estimated,
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

// =============================================================================
// KRAKEN TICKER — HBAR/USD spot price
// =============================================================================
// Pulled from Kraken's free public ticker (no auth) so the dashboard can
// denominate the network-fee run-rate in USD. On-brand for the Kraken-CLI
// stack, and supplementary: a price-fetch failure returns null and the card
// simply falls back to the native ℏ figure. `c[0]` is the last trade price.
// =============================================================================

async function fetchHbarPriceUsd() {
  const url = `${KRAKEN_TICKER_BASE}/0/public/Ticker?pair=HBARUSD`;
  const data = await fetchJson(url);
  if (Array.isArray(data?.error) && data.error.length > 0) {
    throw new Error(`Kraken ticker error: ${data.error.join('; ')}`);
  }
  const result = data?.result || {};
  // Kraken echoes the requested pair as the key, but grab defensively in case
  // it ever canonicalizes to a variant.
  const entry = result.HBARUSD || Object.values(result)[0] || null;
  const last  = entry?.c?.[0];           // c = [last_trade_price, lot_volume]
  const price = last != null ? parseFloat(last) : null;
  return { hbar_usd: sane(price) };
}

// Public fetcher for HBAR — runs blocks + supply + USD price in parallel, then
// sweeps fees across the block window (fees need the window bounds, so they run
// after). Null fields where individual subfetches failed.
export async function fetchHbarActivity() {
  const [blocksResult, supplyResult, priceResult] = await Promise.allSettled([
    fetchHederaBlocks(),
    fetchHederaSupply(),
    fetchHbarPriceUsd(),
  ]);
  const blocks = blocksResult.status === 'fulfilled' ? blocksResult.value : null;
  const supply = supplyResult.status === 'fulfilled' ? supplyResult.value : null;
  const price  = priceResult.status  === 'fulfilled' ? priceResult.value  : null;
  const errors = [];
  if (blocksResult.status === 'rejected') errors.push(`blocks: ${blocksResult.reason?.message || blocksResult.reason}`);
  if (supplyResult.status === 'rejected') errors.push(`supply: ${supplyResult.reason?.message || supplyResult.reason}`);
  if (priceResult.status  === 'rejected') errors.push(`price: ${priceResult.reason?.message || priceResult.reason}`);

  // Fees are scoped to the block window, so they run AFTER blocks resolve.
  // Supplementary: a fee-sweep failure is logged but never sinks the HBAR feed.
  let fees = null;
  if (blocks && blocks.oldest_ts_str && blocks.newest_ts_str) {
    try {
      fees = await fetchHederaFees(blocks.oldest_ts_str, blocks.newest_ts_str);
    } catch (err) {
      errors.push(`fees: ${err.message || String(err)}`);
      fees = null;
    }
  }

  // If BOTH core subfetches failed we treat the whole HBAR feed as unavailable;
  // null signals the pipeline to drop the context block entirely this watch.
  if (!blocks && !supply) return { ok: false, errors };
  return {
    ok:     true,
    blocks, // null if that subfetch failed
    supply, // null if that subfetch failed
    fees,   // { total_fees_hbar, tx_sampled, estimated } — null if unavailable
    price,  // { hbar_usd } — null if the Kraken ticker was unavailable
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
