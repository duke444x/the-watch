// =============================================================================
// ATTEST — Proof-of-Reasoning anchoring on Hedera (HCS)   [v3]
//
// Forward-only. At the end of a successful watch run we take the EXACT canonical
// bytes of that run's "read" (Bridge Log + the structured levels the scorecard
// runs on), sha256 them, and commit the hash to a Hedera Consensus Service topic.
// The on-chain message is tiny — just a hash envelope. The bytes themselves live
// in the ledger (attestations.payload) and get served to the dashboard verifier,
// which re-hashes the identical bytes and checks them against the public mirror
// node. Zero trust in our own server: the hash on the mirror is authoritative.
//
//   the hash proves honesty  — the reasoning existed at time T, unedited
//   the scorecard proves accuracy — kept separate, never conflated
//
// Best-effort and non-blocking: it NEVER throws into the watch pipeline.
//
// ── v3: NO DUPLICATE SUBMITS ────────────────────────────────────────────────
// v2 raced the WHOLE submit (execute + getReceipt) against a timeout. On a slow
// (cold-start) call the race rejected, but the execute() it had already fired
// still reached consensus — and then the sweep, seeing a 'pending' row, fired a
// SECOND tx. Result: two on-chain messages for one run (e.g. run 53 = seq 3 & 4).
//
// v3 splits the two phases:
//   executeSubmit() — sends the tx, NEVER timed, so it is never abandoned.
//   awaitSeq()      — waits for consensus to learn the sequence number; THIS is
//                     the only thing the timeout guards.
// The instant execute() returns we mark the row 'submitted' WITH its tx_id, so
// neither this call nor the sweep can ever fire a second tx. If the receipt is
// slow, the seq is resolved later by hash-matching the mirror (findOnMirrorBySha),
// never by resubmitting. A row is only ever resubmitted while it is still
// 'pending' — i.e. execute() itself failed precheck and no tx was committed.
//
// PUBLIC API (signatures unchanged from v2):
//   attestRun(ledger, runId)            — fire-and-forget anchor + mirror confirm
//   attestRunNow(ledger, runId, opts)   — anchor, await consensus (opts.timeoutMs,
//                                          default 20s), RETURN {ok,seq,sha256,status}
//   sweepPendingAttestations(ledger)    — finish stragglers (resolve seq / confirm)
//   verifyUrl(runId)                    — public read-and-verify URL for a run
//
// NOTES:
//   - env is read lazily (cfg() at call time) — ESM evaluates imports before
//     watch.js runs dotenv.config().
//   - the Hedera Client holds open gRPC handles; each public call closes it in a
//     finally so the (cron-fired) watch process exits cleanly.
// =============================================================================

import crypto from 'crypto';
import {
  Client,
  PrivateKey,
  TopicMessageSubmitTransaction,
} from '@hashgraph/sdk';

const SWEEP_CAP = 10;  // max stragglers handled per run — keeps run tail bounded

function cfg() {
  return {
    network:  (process.env.HEDERA_NETWORK || 'mainnet').toLowerCase(),
    operator: process.env.HEDERA_OPERATOR_ID  || null,
    rawKey:   process.env.HEDERA_OPERATOR_KEY || null,
    topicId:  process.env.HEDERA_TOPIC_ID     || null,
  };
}

function mirrorBase(network) {
  return network === 'mainnet'
    ? 'https://mainnet-public.mirrornode.hedera.com'
    : 'https://testnet.mirrornode.hedera.com';
}

export function isAttestationEnabled() {
  const c = cfg();
  return Boolean(c.operator && c.rawKey && c.topicId);
}

// Public read-and-verify URL for a given run. Used in the Discord Bridge Log
// footer so a reader is one tap from the proof. Base is overridable via env.
export function verifyUrl(runId) {
  const base = (process.env.POR_PUBLIC_BASE || 'https://captsledger.com').replace(/\/+$/, '');
  return `${base}/run/${runId}`;
}

// ---- canonical serialization -----------------------------------------------
// Deterministic JSON: object keys sorted recursively; arrays kept in the order
// the ledger returns them. This string is the single source of truth for the
// hash AND for what the dashboard serves the verifier — byte-identical input.
function canonicalize(value) {
  if (Array.isArray(value)) {
    return '[' + value.map(canonicalize).join(',') + ']';
  }
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).sort();
    return '{' + keys.map(k => JSON.stringify(k) + ':' + canonicalize(value[k])).join(',') + '}';
  }
  return JSON.stringify(value === undefined ? null : value);
}

function sha256hex(str) {
  return crypto.createHash('sha256').update(str, 'utf8').digest('hex');
}

// Resolve a promise but reject if it doesn't settle within ms. v3 wraps ONLY the
// consensus wait (awaitSeq) — never execute() — so a timeout can never abandon
// an in-flight submit.
function withTimeout(promise, ms, label) {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`${label || 'operation'} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

// ---- Hedera client lifecycle ------------------------------------------------
let _client = null;
let _clientErr = null;

async function getClient() {
  if (_client) return _client;
  if (_clientErr) throw _clientErr;
  const c = cfg();
  try {
    // Detect the operator's key type from the ledger so we parse it correctly —
    // an ED25519 key parsed as ECDSA (or vice-versa) yields INVALID_SIGNATURE.
    const res = await fetch(`${mirrorBase(c.network)}/api/v1/accounts/${c.operator}`);
    if (!res.ok) throw new Error(`mirror account lookup ${res.status}`);
    const info = await res.json();
    const keyType = info?.key?._type || 'ED25519';
    const key = keyType === 'ECDSA_SECP256K1'
      ? PrivateKey.fromStringECDSA(c.rawKey)
      : PrivateKey.fromStringED25519(c.rawKey);
    const client = c.network === 'mainnet' ? Client.forMainnet() : Client.forTestnet();
    client.setOperator(c.operator, key);
    _client = client;
    return _client;
  } catch (e) {
    _clientErr = e;
    throw e;
  }
}

function closeClient() {
  if (_client) {
    try { _client.close(); } catch { /* ignore */ }
  }
  _client = null;
  _clientErr = null;
}

// ---- on-chain submit, split in two -----------------------------------------
// Phase 1: send the tx. NOT timed — once this resolves the message is committed
// to the network and will reach consensus, so it must never be abandoned. If it
// throws, precheck rejected it and NOTHING was committed (safe to retry).
async function executeSubmit(sha256, runId) {
  const c = cfg();
  const client = await getClient();   // cold-start cost lives here, untimed
  const envelope = JSON.stringify({ p: 'watch-por', v: 1, run: runId, sha256 });
  const tx = await new TopicMessageSubmitTransaction()
    .setTopicId(c.topicId)
    .setMessage(envelope)
    .execute(client);
  return tx;                          // tx.transactionId is available now
}

// Phase 2: wait for consensus to learn the sequence number. Bounded — this is
// the ONLY thing the timeout guards. A timeout here leaves a fully-recorded
// 'submitted' row; the seq gets filled later by hash-match, not by resubmitting.
async function awaitSeq(tx, ms) {
  const client = await getClient();
  const receipt = await withTimeout(tx.getReceipt(client), ms, 'getReceipt');
  return Number(receipt.topicSequenceNumber.toString());
}

// ---- mirror lookups ---------------------------------------------------------
// Confirm a known sequence number: returns the consensus timestamp iff the
// on-chain hash matches what we stored (else null — mirror lag or mismatch).
async function confirmOnMirror(sequenceNumber, expectedSha256) {
  const c = cfg();
  try {
    const url = `${mirrorBase(c.network)}/api/v1/topics/${c.topicId}/messages/${sequenceNumber}`;
    const res = await fetch(url);
    if (!res.ok) return null;
    const msg = await res.json();
    const decoded = Buffer.from(msg.message, 'base64').toString('utf8');
    let onchain;
    try { onchain = JSON.parse(decoded); } catch { return null; }
    if (onchain.sha256 !== expectedSha256) return null;
    return msg.consensus_timestamp || null;
  } catch {
    return null;
  }
}

// Resolve the seq for a tx whose receipt we never caught: scan recent topic
// messages and find the one whose envelope hash matches. Lets us recover the
// sequence number WITHOUT resubmitting (the anti-duplicate path).
async function findOnMirrorBySha(sha256, limit = 25) {
  const c = cfg();
  try {
    const url = `${mirrorBase(c.network)}/api/v1/topics/${c.topicId}/messages?limit=${limit}&order=desc`;
    const res = await fetch(url);
    if (!res.ok) return null;
    const data = await res.json();
    for (const m of (data.messages || [])) {
      try {
        const env = JSON.parse(Buffer.from(m.message, 'base64').toString('utf8'));
        if (env && env.sha256 === sha256) {
          return { seq: m.sequence_number, consensusTs: m.consensus_timestamp || null };
        }
      } catch { /* skip non-JSON / foreign messages */ }
    }
    return null;
  } catch {
    return null;
  }
}

// ---- internal: ensure a row exists for this run -----------------------------
// Builds the canonical payload and inserts a pending row if none exists yet.
// Returns null when there's nothing to anchor (no Bridge Log for this run).
function ensureRow(ledger, runId) {
  const existing = ledger.getAttestationByRun(runId);
  if (existing) return { row: existing, sha256: existing.sha256 };
  const read = ledger.getRunForAttestation(runId);
  if (!read) return null;
  const payload = canonicalize(read);
  const sha256 = sha256hex(payload);
  ledger.insertAttestation(runId, read.kind || 'bridge_log_v1', payload, sha256);
  return { row: ledger.getAttestationByRun(runId), sha256 };
}

// ---- internal: drive a run to 'submitted' (+ seq if we can), NO double-fire --
// Returns { seq|null, sha256, status, txId } or null (nothing to anchor).
// Caller owns closing the client.
async function ensureSubmitted(ledger, runId, receiptMs) {
  const ensured = ensureRow(ledger, runId);
  if (!ensured) return null;
  const { row, sha256 } = ensured;

  if (row.status === 'submitted') {
    // A tx was already sent for this run — NEVER resubmit. Just learn the seq.
    if (row.sequence_number) {
      return { seq: row.sequence_number, sha256, status: 'submitted', txId: row.tx_id };
    }
    const found = await findOnMirrorBySha(sha256);
    if (found) {
      ledger.markAttestationSubmitted(runId, cfg().topicId, found.seq, row.tx_id);
      if (found.consensusTs) ledger.markAttestationConfirmed(runId, found.consensusTs);
      return { seq: found.seq, sha256, status: 'submitted', txId: row.tx_id };
    }
    return { seq: null, sha256, status: 'submitted', txId: row.tx_id };  // still in flight
  }

  // 'pending' → execute() did not previously succeed, so sending now is safe.
  const tx = await executeSubmit(sha256, runId);
  const txId = tx.transactionId.toString();
  // Record the tx IMMEDIATELY: status flips to 'submitted', so from here on no
  // path (this call or the sweep) will ever fire a second tx for this run.
  ledger.markAttestationSubmitted(runId, cfg().topicId, null, txId);
  try {
    const seq = await awaitSeq(tx, receiptMs);
    ledger.markAttestationSubmitted(runId, cfg().topicId, seq, txId);  // fill seq
    return { seq, sha256, status: 'submitted', txId };
  } catch {
    // receipt slow — tx is sent + recorded; the sweep resolves the seq by hash.
    return { seq: null, sha256, status: 'submitted', txId };
  }
}

// ---- public: attest one run (fire-and-forget) -------------------------------
export async function attestRun(ledger, runId) {
  if (!isAttestationEnabled() || !ledger || !runId) return;
  try {
    const existing = ledger.getAttestationByRun(runId);
    if (existing && existing.status === 'confirmed') return;
    const r = await ensureSubmitted(ledger, runId, 20000);
    if (r && r.seq != null) {
      const ts = await confirmOnMirror(r.seq, r.sha256);
      if (ts) ledger.markAttestationConfirmed(runId, ts);
    }
  } catch {
    // never throw into the pipeline
  } finally {
    closeClient();
  }
}

// ---- public: attest one run SYNCHRONOUSLY, returning the on-chain coords -----
// Submits and awaits consensus so the caller gets the real sequence number back
// BEFORE it posts the Bridge Log — letting the log carry its own proof with no
// after-the-fact edit. Bounded by opts.timeoutMs (default 20s, comfortably above
// finality even cold). On a slow receipt: returns { ok:false } but the row is
// fully 'submitted' (tx sent, never lost) and the sweep finishes it.
//
// Returns: { ok, runId, seq, sha256, status }
export async function attestRunNow(ledger, runId, opts = {}) {
  const receiptMs = Number.isFinite(opts.timeoutMs) ? opts.timeoutMs : 20000;
  const out = { ok: false, runId, seq: null, sha256: null, status: null };
  if (!isAttestationEnabled() || !ledger || !runId) return out;
  try {
    const existing = ledger.getAttestationByRun(runId);
    if (existing && existing.status === 'confirmed') {
      return { ok: true, runId, seq: existing.sequence_number, sha256: existing.sha256, status: 'confirmed' };
    }
    const r = await ensureSubmitted(ledger, runId, receiptMs);
    if (!r) return out;                       // nothing to anchor
    return { ok: r.seq != null, runId, seq: r.seq, sha256: r.sha256, status: r.status };
  } catch (e) {
    try { ledger.markAttestationError(runId, e.message); } catch { /* ignore */ }
    return out;
  } finally {
    closeClient();
  }
}

// ---- public: retry stragglers ----------------------------------------------
// 'pending' rows (execute never succeeded) get sent. 'submitted' rows get their
// seq resolved (by hash) and/or confirmed against the mirror. Bounded by SWEEP_CAP.
export async function sweepPendingAttestations(ledger) {
  if (!isAttestationEnabled() || !ledger) return;
  let rows;
  try { rows = ledger.getRetryableAttestations(SWEEP_CAP); } catch { return; }
  try {
    for (const row of rows || []) {
      try {
        if (row.status === 'pending') {
          // No tx has been committed for this row — sending now cannot duplicate.
          const tx = await executeSubmit(row.sha256, row.run_id);
          const txId = tx.transactionId.toString();
          ledger.markAttestationSubmitted(row.run_id, cfg().topicId, null, txId);
          try {
            const seq = await awaitSeq(tx, 20000);
            ledger.markAttestationSubmitted(row.run_id, cfg().topicId, seq, txId);
            const ts = await confirmOnMirror(seq, row.sha256);
            if (ts) ledger.markAttestationConfirmed(row.run_id, ts);
          } catch {
            // seq pending — next sweep resolves it by hash
          }
        } else if (row.status === 'submitted') {
          if (row.sequence_number) {
            const ts = await confirmOnMirror(row.sequence_number, row.sha256);
            if (ts) ledger.markAttestationConfirmed(row.run_id, ts);
          } else {
            const found = await findOnMirrorBySha(row.sha256);
            if (found) {
              ledger.markAttestationSubmitted(row.run_id, cfg().topicId, found.seq, row.tx_id);
              if (found.consensusTs) ledger.markAttestationConfirmed(row.run_id, found.consensusTs);
            }
          }
        }
      } catch {
        // keep sweeping the rest
      }
    }
  } finally {
    closeClient();
  }
}

// exported for the dashboard verify endpoint / tests
export { canonicalize, sha256hex };
