// =============================================================================
// CAPT. CRAWL'S BRIDGE — Client
// Connects to the server's SSE stream and animates the dashboard in real time:
// terminal output, bridge log streaming, plank marker, trade fields, tickers.
// =============================================================================

(() => {
'use strict';

// -- DOM refs --------------------------------------------------------------
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const connectionStatus = $('#connectionStatus');
const connectionDot    = connectionStatus.querySelector('.status-dot');
const connectionText   = connectionStatus.querySelector('.status-text');
const serverTime       = $('#serverTime');

const tickerEls = {
  HBAR: document.querySelector('.ticker[data-asset="HBAR"]'),
  BTC:  document.querySelector('.ticker[data-asset="BTC"]'),
  DOG:  document.querySelector('.ticker[data-asset="DOG"]'),
};

const terminalBody = $('#terminalBody');
const stepper      = $('#stepper');
const stepCells    = $$('.stepper-cell');
const runBtn       = $('#runBtn');
const forceMode    = $('#forceMode');

const logTitle    = $('#logTitle');
const logBody     = $('#logBody');
const logPulse    = $('#logPulse');
const tradeFields = $('#tradeFields');
const tradePair   = $('#tradePair');
const tradeFill   = $('#tradeFill');
const tradeMarker = $('#tradeMarker');
const discordBadge = $('#discordBadge');

const plankCurrentLabel = $('#plankCurrentLabel');
const plankMarker       = $('#plankMarker');
const plankPositions    = $$('.plank-position');

// -- Layout adjustment: give the bridge log more breathing room ------------
// Override CSS default so Capt's longer multi-paragraph logs fit on screen.
if (logBody) {
  logBody.style.maxHeight = '600px';
}

// -- State -----------------------------------------------------------------
let collectingLog = false;
let logBuffer = '';
let placeholderCleared = false;
let logPlaceholderCleared = false;
let lastForcedTrade = null;
let lastFillData = null;

// -- Utilities -------------------------------------------------------------
function clearTerminalPlaceholder() {
  if (placeholderCleared) return;
  const ph = terminalBody.querySelector('.terminal-placeholder');
  if (ph) ph.remove();
  placeholderCleared = true;
}
function clearLogPlaceholder() {
  if (logPlaceholderCleared) return;
  const ph = logBody.querySelector('.log-placeholder');
  if (ph) ph.remove();
  logPlaceholderCleared = true;
}
function setConnection(state, label) {
  connectionStatus.classList.remove('is-running', 'is-error');
  if (state === 'running') connectionStatus.classList.add('is-running');
  if (state === 'error')   connectionStatus.classList.add('is-error');
  connectionText.textContent = label;
}
function fmtPrice(v) {
  if (v == null) return '—';
  if (v < 0.01) return '$' + v.toFixed(6);
  if (v < 1)    return '$' + v.toFixed(5);
  if (v < 100)  return '$' + v.toFixed(4);
  return '$' + v.toLocaleString(undefined, { maximumFractionDigits: 2 });
}
function fmtPct(v) {
  if (v == null || isNaN(v)) return '—';
  const sign = v >= 0 ? '+' : '';
  return sign + v.toFixed(2) + '%';
}

// -- Tickers --------------------------------------------------------------
function updateTicker(asset, data) {
  const el = tickerEls[asset];
  if (!el || !data) return;
  const priceEl  = el.querySelector('[data-field="last"]');
  const changeEl = el.querySelector('[data-field="change"]');
  const rangeEl  = el.querySelector('[data-field="range"]');

  if (priceEl) {
    const prev = priceEl.dataset.value;
    const next = String(data.last);
    priceEl.textContent = fmtPrice(data.last);
    priceEl.dataset.value = next;
    if (prev && prev !== next) {
      priceEl.classList.remove('flash');
      void priceEl.offsetWidth;
      priceEl.classList.add('flash');
      setTimeout(() => priceEl.classList.remove('flash'), 900);
    }
  }
  if (changeEl) {
    changeEl.textContent = fmtPct(data.changePct);
    changeEl.classList.remove('up', 'down');
    changeEl.classList.add(data.changePct >= 0 ? 'up' : 'down');
  }
  if (rangeEl) {
    rangeEl.textContent = `${fmtPrice(data.low24)} – ${fmtPrice(data.high24)}`;
  }
}

// -- Stepper --------------------------------------------------------------
function showStepper() { stepper.hidden = false; }
function resetStepper() {
  stepCells.forEach((c) => { c.classList.remove('is-active', 'is-done'); });
}
function setStep(num) {
  stepCells.forEach((c) => {
    const n = parseInt(c.dataset.step, 10);
    c.classList.remove('is-active', 'is-done');
    if (n < num) c.classList.add('is-done');
    else if (n === num) c.classList.add('is-active');
  });
}
function completeAllSteps() {
  stepCells.forEach((c) => { c.classList.remove('is-active'); c.classList.add('is-done'); });
}

// -- Terminal rendering ---------------------------------------------------
function classifyLine(plain) {
  const stripped = plain.trim();
  if (/^\[\d+\/\d+\]/.test(stripped))               return 'step-line';
  if (/^→\s/.test(stripped))                         return 'action-line';
  if (/^✓\s/.test(stripped))                         return 'ok-line';
  if (/^✗\s/.test(stripped))                         return 'warn-line';
  if (/^\$\skraken/.test(stripped))                  return 'cmd-line';
  if (/^—\s/.test(stripped))                         return '';
  return '';
}

function appendTerminalLine(rawLine, plainLine) {
  clearTerminalPlaceholder();
  const plain = plainLine || rawLine;
  if (plain == null) return;
  // Skip empty lines unless they're inside the log section
  if (plain.trim() === '' && !collectingLog) return;

  const div = document.createElement('div');
  div.className = 'terminal-line ' + classifyLine(plain);
  // For cmd lines we already prefix via ::before, strip the literal "$ " from the visible content
  if (div.classList.contains('cmd-line')) {
    div.textContent = plain.replace(/^\s*\$\s+/, '');
  } else if (div.classList.contains('ok-line')) {
    div.textContent = plain.replace(/^\s*✓\s+/, '');
  } else if (div.classList.contains('action-line')) {
    div.textContent = plain.replace(/^\s*→\s+/, '');
  } else if (div.classList.contains('warn-line')) {
    div.textContent = plain.replace(/^\s*✗\s+/, '');
  } else {
    div.textContent = plain;
  }
  terminalBody.appendChild(div);
  // Auto-scroll to bottom
  terminalBody.scrollTop = terminalBody.scrollHeight;

  // Bridge Log section tracking — content between log header and the divider
  if (collectingLog) {
    if (/━{5,}/.test(plain)) {
      // End-of-log divider
      collectingLog = false;
      logPulse.hidden = true;
      finalizeLogBody();
    } else if (!/📡 Bridge Log/.test(plain)) {
      logBuffer += plain + '\n';
      renderLogBuffer();
    }
  } else if (/━{5,}/.test(plain)) {
    // Start divider before the log — ignore the divider itself
    // (collectingLog gets set true on log_start event)
  }
}

// -- Bridge Log rendering -------------------------------------------------
function startLog(header) {
  clearLogPlaceholder();
  // Parse "📡 Bridge Log — May 23, 16:48 Central" → "May 23, 16:48 Central"
  const match = /📡 Bridge Log\s*—\s*(.+)/.exec(header);
  logTitle.textContent = match ? match[1].trim() : header.trim();
  logBuffer = '';
  logBody.innerHTML = '';
  logPulse.hidden = false;
  collectingLog = true;
}

function renderLogBuffer() {
  // Very light markdown: paragraphs by blank line, **bold**, *italic*, --- as <hr>, list items prefixed by - or *
  let html = '';
  const blocks = logBuffer.split(/\n\n+/);
  for (const block of blocks) {
    const trimmed = block.trim();
    if (!trimmed) continue;
    if (/^-{3,}$/.test(trimmed)) {
      html += '<hr>';
      continue;
    }
    // Lines within a block become a single paragraph with <br>
    const lines = trimmed.split('\n').map(escapeAndFormat).join('<br>');
    html += `<p>${lines}</p>`;
  }
  // Add a blinking cursor at the end if still collecting
  if (collectingLog) html += '<span class="log-cursor" aria-hidden="true"></span>';
  logBody.innerHTML = html;
  // While streaming, follow the latest text at the bottom of the card
  logBody.scrollTop = logBody.scrollHeight;
}

function escapeAndFormat(line) {
  // Escape HTML
  let s = line
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
  // Bold **...**
  s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
  // Italic *...*
  s = s.replace(/(^|\s)\*([^*]+)\*(?=\s|$|[,.;:])/g, '$1<em>$2</em>');
  return s;
}

function finalizeLogBody() {
  collectingLog = false;
  // Remove blinking cursor by re-rendering once more without it
  renderLogBuffer();
  // Now sweep cursor out
  const cur = logBody.querySelector('.log-cursor');
  if (cur) cur.remove();
}

// -- Trade fields ---------------------------------------------------------
function showTradeFields(execution) {
  tradePair.textContent = `${execution.pair || ''}`.trim() || '—';
  tradeFill.textContent = execution.fillStr || '—';
  tradeMarker.textContent = execution.markerStr || '—';
  tradeFields.hidden = false;
}

// -- Plank ---------------------------------------------------------------
const POS_INDEX = {
  'deck':       0,
  'the rail':   1,
  'one out':    2,
  'two out':    3,
  'three out':  4,
  'the edge':   5,
};
function setPlankPosition(label) {
  const key = (label || 'deck').toLowerCase().replace(/\s+/g, ' ').trim();
  const idx = POS_INDEX[key];
  if (idx == null) return;
  plankPositions.forEach((el, i) => {
    el.classList.toggle('is-current', i === idx);
  });
  // Position the marker — 6 columns, marker centered on column (idx + 0.5) / 6
  const pct = ((idx + 0.5) / 6) * 100;
  plankMarker.style.left = pct + '%';
  // Update label
  if (idx === 0) {
    plankCurrentLabel.textContent = 'Marker on the deck';
  } else {
    const labels = ['deck', 'the rail', 'one out', 'two out', 'three out', 'the edge'];
    plankCurrentLabel.textContent = `Marker at ${labels[idx]}`;
  }
}

// -- Clock ----------------------------------------------------------------
function updateClock() {
  const d = new Date();
  const time = d.toLocaleTimeString('en-US', {
    timeZone: 'America/Chicago', hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
  serverTime.textContent = time + ' CT';
}
setInterval(updateClock, 1000); updateClock();

// -- SSE connection -------------------------------------------------------
let es = null;
function connectSSE() {
  if (es) try { es.close(); } catch (_) {}
  es = new EventSource('/events');

  es.addEventListener('hello',  () => setConnection('ok', 'Connected'));
  es.addEventListener('error',  () => setConnection('error', 'Disconnected'));

  es.addEventListener('tickers', (e) => {
    const data = JSON.parse(e.data);
    if (data.HBAR) updateTicker('HBAR', data.HBAR);
    if (data.BTC)  updateTicker('BTC',  data.BTC);
    if (data.DOG)  updateTicker('DOG',  data.DOG);
  });

  es.addEventListener('run_start', (e) => {
    const data = JSON.parse(e.data);
    runBtn.disabled = true;
    setConnection('running', data.forced ? 'Running · forced' : 'Running');
    showStepper();
    resetStepper();
    // Clear terminal for a new run
    terminalBody.innerHTML = '';
    placeholderCleared = true;
    lastForcedTrade = data.forced ? data.args : null;
    tradeFields.hidden = true;
    discordBadge.hidden = true;
    appendTerminalLine('', `▼ New watch session — ${new Date().toLocaleTimeString()}`);
  });

  // The 'terminal' event handler below appends every line to the scroller.
  // The 'step' / 'command' / 'decision' handlers are STATE-ONLY — they
  // must NOT also append text, or each styled line shows up twice.
  es.addEventListener('step', (e) => {
    const data = JSON.parse(e.data);
    setStep(data.num);
    // Line text is appended via the 'terminal' event handler below.
  });

  es.addEventListener('command', (e) => {
    // State-only handler. Line text is appended via 'terminal' event.
  });

  es.addEventListener('terminal', (e) => {
    const data = JSON.parse(e.data);
    appendTerminalLine(data.raw, data.plain);
  });

  es.addEventListener('decision', (e) => {
    // State-only handler. Line text is appended via 'terminal' event.
  });

  es.addEventListener('trade_filled', (e) => {
    const data = JSON.parse(e.data);
    lastFillData = data;
  });

  es.addEventListener('plank_advance', (e) => {
    const data = JSON.parse(e.data);
    setPlankPosition(data.position);
    // Populate trade fields if we have fill info
    if (lastFillData) {
      const pairText = (lastForcedTrade && lastForcedTrade[2]) ? lastForcedTrade[2] : 'HBARUSD';
      // Actually parse from recent action; fallback to HBARUSD
      showTradeFields({
        pair: pairText + ' · ' + (data.position || ''),
        fillStr: `${fmtPrice(lastFillData.price)} · cost $${lastFillData.cost.toFixed(2)}`,
        markerStr: data.position,
      });
    } else {
      showTradeFields({
        pair: 'Position open',
        fillStr: '—',
        markerStr: data.position,
      });
    }
  });

  es.addEventListener('log_start', (e) => {
    const data = JSON.parse(e.data);
    startLog(data.header);
  });

  es.addEventListener('discord_posted', () => {
    discordBadge.hidden = false;
  });

  es.addEventListener('run_complete', () => {
    runBtn.disabled = false;
    setConnection('ok', 'Connected · idle');
    completeAllSteps();
    if (collectingLog) { collectingLog = false; logPulse.hidden = true; finalizeLogBody(); }
    // Once the run is fully complete, scroll the bridge log back to the
    // top so viewers can read Capt's full analysis from the beginning.
    // setTimeout lets finalizeLogBody settle the DOM first.
    setTimeout(() => {
      if (logBody) logBody.scrollTo({ top: 0, behavior: 'smooth' });
    }, 350);
  });

  es.addEventListener('run_error', (e) => {
    runBtn.disabled = false;
    setConnection('error', 'Run failed');
    const data = JSON.parse(e.data);
    appendTerminalLine('', `✗ ${data.message}`);
  });

  es.addEventListener('stderr', (e) => {
    const data = JSON.parse(e.data);
    appendTerminalLine('', `[stderr] ${data.line.trim()}`);
  });

  es.onerror = () => {
    setConnection('error', 'Reconnecting…');
    setTimeout(connectSSE, 2500);
  };
}

// -- Run button -----------------------------------------------------------
runBtn.addEventListener('click', async () => {
  const sel = forceMode.value;
  const body = {};
  if (sel) {
    const [pair, size] = sel.split(':');
    body.forceEnter = { pair, size };
  }
  try {
    const res = await fetch('/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      appendTerminalLine('', `✗ ${err.error || 'Run rejected'}`);
    }
  } catch (e) {
    appendTerminalLine('', `✗ Network error: ${e.message}`);
  }
});

// -- Latest log preload ---------------------------------------------------
async function preloadLatestLog() {
  try {
    const res = await fetch('/latest-log');
    const data = await res.json();
    if (!data.available || !data.content) return;
    clearLogPlaceholder();
    const firstLine = data.content.split('\n')[0] || '';
    const match = /📡 Bridge Log\s*—\s*(.+)/.exec(firstLine);
    logTitle.textContent = (match ? match[1].trim() : firstLine.trim()) + '  ·  saved';
    // Render the markdown of the saved log
    const restored = data.content.replace(/^📡 Bridge Log.*\n/, '').trim();
    logBuffer = restored + '\n';
    collectingLog = false;
    renderLogBuffer();
    // Remove the blinking cursor that renderLogBuffer added if collectingLog was true
    const cur = logBody.querySelector('.log-cursor');
    if (cur) cur.remove();
    // Preloaded saved log starts at the top so viewers can read it from
    // the beginning instead of mid-paragraph.
    logBody.scrollTop = 0;
  } catch (_) { /* ignore */ }
}

// -- Bootstrap ------------------------------------------------------------
connectSSE();
preloadLatestLog();

})();
