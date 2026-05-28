// =============================================================================
// SCHEDULER — The Watch's cron-driven runner
//
// Long-running PM2 process that fires watch.js and exits-only.js on a fixed
// schedule. Four runs daily by default:
//
//   9:00 AM CT — Morning Bridge       (watch.js)
//   3:00 PM CT — Midday Exit-Check    (exits-only.js, silent)
//   9:00 PM CT — Evening Wrap         (watch.js)
//   3:00 AM CT — Overnight Exit-Check (exits-only.js, silent)
//
// Design notes:
//   - node-cron handles timezone + DST correctly
//   - Each scheduled fire is a fresh `node` process so memory doesn't accumulate
//   - Single-flight lock: if a previous run is still in progress when the
//     next tick fires, the new fire is skipped (and logged) — keeps the
//     paper account from racing
//   - Scheduler doesn't touch the ledger directly — child processes handle
//     their own persistence. Scheduler logs go to PM2 stdout for observability.
//
// PM2 ops:
//   pm2 start /home/capt-crawl/watch/scheduler.js --name watch-scheduler
//   pm2 logs watch-scheduler
//   pm2 restart watch-scheduler
// =============================================================================

import cron from 'node-cron';
import { spawn } from 'child_process';
import { existsSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TZ = 'America/Chicago';
const PAUSE_FLAG = path.join(__dirname, 'data', 'PAUSED.flag');

// =============================================================================
// SCHEDULES
// =============================================================================
// Each entry pairs a cron expression (evaluated in TZ) with a script to spawn
// and a source tag that gets passed as --source for ledger run_type tagging.
// =============================================================================

const SCHEDULES = [
  // Bridge Log runs — full watch.js pipeline with narration
  {
    name:   'scheduled_morning',
    cron:   '0 9 * * *',
    label:  'Morning Bridge (9:00 AM CT)',
    script: 'watch.js',
  },
  {
    name:   'scheduled_evening',
    cron:   '0 21 * * *',
    label:  'Evening Wrap (9:00 PM CT)',
    script: 'watch.js',
  },
  // Silent exit-checks — exits-only.js, no Bridge Log
  {
    name:   'exit_check_midday',
    cron:   '0 15 * * *',
    label:  'Midday Exit-Check (3:00 PM CT)',
    script: 'exits-only.js',
  },
  {
    name:   'exit_check_overnight',
    cron:   '0 3 * * *',
    label:  'Overnight Exit-Check (3:00 AM CT)',
    script: 'exits-only.js',
  },
];

// =============================================================================
// HELPERS
// =============================================================================

function nowCentral() {
  const now = new Date();
  return now.toLocaleString('en-US', {
    timeZone: TZ,
    year: 'numeric', month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }) + ' CT';
}

function log(msg) {
  console.log(`[${nowCentral()}] ${msg}`);
}

// =============================================================================
// FIRE — spawn the target script as a subprocess
// =============================================================================

// Single-flight lock. If a previous run is still going when the next cron tick
// fires, we skip the new one and log it. Better to drop a tick than race the
// paper account.
let isRunning = false;
let activeRunLabel = null;

function fireScript(scriptName, source, label) {
  return new Promise((resolve) => {
    if (existsSync(PAUSE_FLAG)) {
      log(`⏸ SKIP — ${label} declined; pause flag is set (use !watch-resume to clear)`);
      return resolve({ skipped: true, reason: 'paused' });
    }
    if (isRunning) {
      log(`⚠ SKIP — ${label} declined; previous run still in progress (${activeRunLabel})`);
      return resolve({ skipped: true, reason: 'in_flight' });
    }

    isRunning = true;
    activeRunLabel = label;
    log(`▶ FIRE — ${label}  [script=${scriptName} source=${source}]`);
    const startTime = Date.now();

    const scriptPath = path.join(__dirname, scriptName);
    const proc = spawn('node', [scriptPath, '--source', source], {
      cwd: __dirname,
      stdio: ['ignore', 'inherit', 'inherit'],
      env: process.env,
    });

    proc.on('exit', (code, signal) => {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      if (code === 0) {
        log(`✓ DONE — ${label} completed in ${elapsed}s`);
      } else if (signal) {
        log(`✗ FAIL — ${label} killed by signal ${signal} after ${elapsed}s`);
      } else {
        log(`✗ FAIL — ${label} exited with code ${code} after ${elapsed}s`);
      }
      isRunning = false;
      activeRunLabel = null;
      resolve({ code, signal, elapsed });
    });

    proc.on('error', (err) => {
      log(`✗ FAIL — ${label} could not spawn: ${err.message}`);
      isRunning = false;
      activeRunLabel = null;
      resolve({ error: err.message });
    });
  });
}

// =============================================================================
// STARTUP BANNER
// =============================================================================

log('═══════════════════════════════════════════════════════════');
log('🏴‍☠️  THE WATCH — Scheduler online');
log(`    Working dir:  ${__dirname}`);
log(`    Timezone:     ${TZ}`);
if (existsSync(PAUSE_FLAG)) {
  log(`    ⏸ PAUSED       — scheduled fires will be skipped until pause flag is cleared`);
}
log('    Schedule:');
for (const s of SCHEDULES) {
  log(`      • ${s.cron.padEnd(12)}  ${s.label.padEnd(36)}  [${s.script}]`);
}
log('═══════════════════════════════════════════════════════════');

// =============================================================================
// REGISTER CRON JOBS
// =============================================================================

for (const { name, cron: expr, label, script } of SCHEDULES) {
  const valid = cron.validate(expr);
  if (!valid) {
    log(`✗ FATAL — invalid cron expression for ${name}: "${expr}"`);
    process.exit(1);
  }
  cron.schedule(expr, () => {
    fireScript(script, name, label).catch(err => {
      log(`✗ Unexpected error in fireScript (${name}): ${err.message}`);
    });
  }, { timezone: TZ });
}

log(`Scheduler is now idle, waiting for next scheduled fire...`);

// =============================================================================
// GRACEFUL SHUTDOWN
// =============================================================================

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  log(`${signal} received, shutting down scheduler`);
  const deadline = Date.now() + 60_000;
  while (isRunning && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, 500));
  }
  if (isRunning) {
    log(`Shutdown timeout reached — exiting with a run still in flight (${activeRunLabel})`);
  } else {
    log('Clean shutdown.');
  }
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
