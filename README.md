# The Watch

An on-chain stacking agent for holders with conviction, built on the [Kraken CLI](https://github.com/krakenfx/kraken-cli).

Submitted to Kraken Agent Zero on May 24, 2026.

## Positioning

> Most trading agents try to make you more dollars.
> The Watch tries to make you more of the tokens you actually want to hold long-term.

For holders, not flippers.

## What it does

The Watch is run by Capt. Crawl, an AI agent powered by Claude Sonnet 4. It fires twice daily (9 AM and 9 PM Central) plus on-demand exit checks. Every watch, Capt evaluates the portfolio across 5 pairs (HBAR, BTC, DOG, SOL, SUI) and three timeframes (15-min intraday, 4h swing, 1d macro), reads order book depth, pulls on-chain context for the stacking targets, and files a structured Bridge Log explaining what he sees and why.

The agent maintains a paper account and an immutable trade ledger. Every closed trade — with its original thesis attached — feeds back into the next decision, so Capt can spot when he's running a thesis that's already failed twice.

## Differentiators

**Multi-timeframe reasoning.** The same setup at the floor of a 7-day range in a confirmed 30-day uptrend has different odds than the same setup in a 30-day downtrend. Capt sees all three.

**On-chain context for stacking targets.** Cross-source intelligence the price chart can't show:

- **HBAR** via [Hedera Mirror Node](https://mainnet-public.mirrornode.hedera.com) — network TPS, transaction throughput, gas usage, supply
- **DOG** via [Unisat](https://open-api.unisat.io) — holders, runes-marketplace activity, sat-floor

Capt watches for divergences. Network busy + price flat = accumulation under the price. Holders growing + sat-floor weak = quiet stacking. Holders dropping + price holds = distribution.

**Self-consistency tracking.** Capt sees his own last 5 reads per pair each fire. Consistency across a clear signal is strength; drift without new information is a warning sign he's expected to name in his factors.

**Track-record awareness.** Every closed trade with its original thesis feeds back. When the same playbook fails twice, Capt knows. Recent example from Bridge Log Run #21: *"Two losses from the same playbook is a pattern, not bad luck."*

**Stack-quantity scoreboard.** Token-quantity tracking for HBAR and DOG. The dollar account is a vehicle; the stack is the metric.

**Sat-native DOG framing.** DOG is a Bitcoin Runes token; native denomination is sats per unit. The dashboard and Capt's reasoning both treat sat-floor as the conviction signal, USD as the translation.

**Discipline-focused voice.** Capt narrates each watch in plain language for the holders following along, with accountability for his past calls.

## Live dashboard

[http://198.199.73.11:4444](http://198.199.73.11:4444)

## Architecture

```
Kraken CLI ──┐
             ├─→ Decision pipeline ─→ Paper account
Hedera ──────┤        (Claude)
Unisat ──────┘
              └─→ SQLite ledger ─→ Dashboard
              └─→ Bridge Log     ─→ Discord
```

Each watch is a 10-step pipeline:

1. Ticker snapshots across 5 pairs
2. Order book depth + liquidity tiering
3. OHLC across 3 timeframes (15m / 4h / 1d)
4. On-chain context (Hedera Mirror Node + Unisat)
5. Paper account state + equity snapshot
6. Ledger feedback (open positions, recent closes, lifetime stats, recent per-pair reads)
7. Portfolio decision (Claude Sonnet 4)
8. Execute actions on paper account
9. Bridge Log narration
10. Broadcast to Discord

## Stack

- Node.js 18+ (native `fetch`, ESM modules)
- `@anthropic-ai/sdk` — Claude Sonnet 4 for decisions and narration
- `better-sqlite3` — ledger persistence
- `express` — dashboard HTTP API
- Kraken CLI (external dependency) — market data + paper trading

## Files

```
watch.js      — main pipeline + decision/narration prompts
ledger.js     — SQLite schema + queries (trades, snapshots, equity, on-chain)
onchain.js    — Hedera Mirror Node + Unisat fetchers
server.js     — dashboard HTTP API
exits.js      — silent exit-check pipeline (3 AM / 3 PM CT cron)
webhooks.js   — Discord webhook routing
public/
  index.html  — single-file dashboard SPA
```

## Running

```bash
cp .env.example .env
# Fill in the keys (Anthropic required; others optional)
npm install
node watch.js
```

## Submission state

The original Agent Zero submission (May 24, 2026) is preserved at tag `v0.1-submission`. The current `main` reflects continued development since, including the on-chain integration, the Capt's Read dashboard panel, multi-timeframe analysis, the stack scoreboard, and self-consistency tracking.

## Credits

Capt. Crawl runs on Claude Sonnet 4. The Watch built by [@duke444x](https://x.com/duke444x) for the [@krakenpro](https://x.com/krakenpro) Agent Zero promotion.

🏴‍☠️ B4E
