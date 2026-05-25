// =============================================================================
// CAPT. CRAWL'S BRIDGE — Server
// Express + Server-Sent Events backend that spawns watch.js as a child process,
// parses its output for semantic events, and broadcasts to connected dashboard
// clients in real time. Also polls Kraken CLI for live tickers.
// =============================================================================

import express from 'express';
import { spawn } from 'child_process';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';

dotenv.config({ quiet: true });

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.DASHBOARD_PORT || 3000;
const TICKER_REFRESH_MS = 30_000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// =============================================================================
// SSE BROADCAST
// =============================================================================

const clients = new Set();
let lastTickers = null;
let runActive = false;

function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of clients) {
    try { client.write(payload); } catch (_) { /* client probably disconnected */ }
  }
}

app.get('/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  res.write(`event: hello\ndata: ${JSON.stringify({ ts: Date.now() })}\n\n`);
  if (lastTickers) {
    res.write(`event: tickers\ndata: ${JSON.stringify(lastTickers)}\n\n`);
  }

  clients.add(res);
  req.on('close', () => clients.delete(res));
});

// =============================================================================
// TERMINAL OUTPUT PARSER — strip ANSI, infer semantic events
// =============================================================================

function stripAnsi(str) {
  return str.replace(/\x1b\[[0-9;]*[a-zA-Z]/g, '');
}

function parseAndBroadcast(rawLine) {
  if (!rawLine && rawLine !== '') return;
  const plain = stripAnsi(rawLine);

  // Always emit the raw line for the terminal panel
  broadcast('terminal', { raw: rawLine, plain });

  // Step header: [N/M] message
  const step = plain.match(/^\[(\d+)\/(\d+)\]\s+(.+)$/);
  if (step) {
    broadcast('step', {
      num: parseInt(step[1], 10),
      total: parseInt(step[2], 10),
      msg: step[3].trim(),
    });
    return;
  }

  // Decision marker
  if (/→\s+(DECISION:|FORCED ENTER)/i.test(plain)) {
    broadcast('decision', { text: plain.replace(/^\s*→\s+/, '').trim() });
    return;
  }

  // Trade fill
  const fill = plain.match(/Filled @ \$(\S+).*cost \$([0-9.]+).*fee \$([0-9.]+)/);
  if (fill) {
    broadcast('trade_filled', {
      price: parseFloat(fill[1]),
      cost: parseFloat(fill[2]),
      fee: parseFloat(fill[3]),
    });
    return;
  }

  // Plank advancement
  const plank = plain.match(/Marker advanced to:\s+(.+)$/);
  if (plank) {
    broadcast('plank_advance', { position: plank[1].trim().toLowerCase() });
    return;
  }

  // Bridge Log header
  if (plain.includes('📡 Bridge Log')) {
    broadcast('log_start', { header: plain.trim() });
    return;
  }

  // Discord post confirmation
  if (/Posted to Discord/.test(plain)) {
    broadcast('discord_posted', { ok: true });
    return;
  }

  // Kraken CLI command (preceded by '$ kraken ')
  const cmd = plain.match(/^\s*\$\s+(kraken\s+.+)$/);
  if (cmd) {
    broadcast('command', { cmd: cmd[1].trim() });
    return;
  }
}

// =============================================================================
// WATCH RUNNER — spawn watch.js, stream output
// =============================================================================

app.post('/run', (req, res) => {
  if (runActive) {
    res.status(409).json({ error: 'A watch is already running. Wait for it to finish.' });
    return;
  }

  const { forceEnter } = req.body || {};
  const args = ['watch.js'];
  if (forceEnter && forceEnter.pair && forceEnter.size) {
    args.push('--force-enter', forceEnter.pair, forceEnter.size);
  }

  runActive = true;
  broadcast('run_start', { args, forced: !!forceEnter });

  const proc = spawn('node', args, {
    cwd: __dirname,
    env: process.env,
  });

  let stdoutBuf = '';
  proc.stdout.on('data', (chunk) => {
    stdoutBuf += chunk.toString();
    const lines = stdoutBuf.split('\n');
    stdoutBuf = lines.pop();
    for (const line of lines) parseAndBroadcast(line);
  });

  proc.stderr.on('data', (chunk) => {
    broadcast('stderr', { line: chunk.toString() });
  });

  proc.on('close', (code) => {
    if (stdoutBuf) parseAndBroadcast(stdoutBuf);
    runActive = false;
    broadcast('run_complete', { code, ts: Date.now() });
  });

  proc.on('error', (e) => {
    runActive = false;
    broadcast('run_error', { message: e.message });
  });

  res.json({ started: true });
});

// =============================================================================
// LIVE TICKERS — poll Kraken CLI every 30s
// =============================================================================

function fetchTickers() {
  return new Promise((resolve) => {
    const proc = spawn('kraken', ['ticker', 'HBARUSD', 'BTCUSD', 'DOGUSD', '-o', 'json']);
    let stdout = '';
    proc.stdout.on('data', (d) => { stdout += d; });
    proc.on('close', () => {
      try { resolve(JSON.parse(stdout)); }
      catch (_) { resolve(null); }
    });
    proc.on('error', () => resolve(null));
  });
}

function summarizeTicker(d) {
  if (!d) return null;
  const last = parseFloat(d.c[0]);
  const open = parseFloat(d.o);
  const high24 = parseFloat(d.h[1]);
  const low24 = parseFloat(d.l[1]);
  const change = ((last - open) / open) * 100;
  return {
    last, open, high24, low24,
    changePct: change,
  };
}

async function pollTickers() {
  const data = await fetchTickers();
  if (!data) return;
  const tickers = {
    HBAR: summarizeTicker(data.HBARUSD),
    BTC: summarizeTicker(data.XXBTZUSD || data.BTCUSD),
    DOG: summarizeTicker(data.DOGUSD),
    ts: Date.now(),
  };
  lastTickers = tickers;
  broadcast('tickers', tickers);
}

setInterval(pollTickers, TICKER_REFRESH_MS);
pollTickers(); // initial

// =============================================================================
// STATE ENDPOINT (for initial load convenience)
// =============================================================================

app.get('/state', (req, res) => {
  res.json({
    tickers: lastTickers,
    runActive,
    serverTime: Date.now(),
  });
});

// Return the most recent saved Bridge Log markdown file
app.get('/latest-log', async (req, res) => {
  try {
    const fs = await import('fs/promises');
    const logsDir = path.join(__dirname, 'logs');
    const entries = await fs.readdir(logsDir).catch(() => []);
    const logs = entries.filter((f) => f.startsWith('bridge-log-') && f.endsWith('.md'));
    if (logs.length === 0) {
      res.json({ available: false });
      return;
    }
    logs.sort().reverse();
    const newest = logs[0];
    const content = await fs.readFile(path.join(logsDir, newest), 'utf-8');
    res.json({ available: true, filename: newest, content });
  } catch (e) {
    res.json({ available: false, error: e.message });
  }
});

// =============================================================================
// START
// =============================================================================

app.listen(PORT, () => {
  console.log(`\n  ⚓  Capt. Crawl's Bridge listening on http://0.0.0.0:${PORT}/\n`);
});
