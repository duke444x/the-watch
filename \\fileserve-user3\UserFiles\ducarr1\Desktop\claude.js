// claude.js — Anthropic API wrapper for Capt. Crawl
// Manages per-channel conversation history with rolling window.
// Supports Anthropic tool use for live market data fetches (step 9).

const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');
const sharp = require('sharp');
const https = require('https');
const http = require('http');
const { SYSTEM_PROMPT } = require('./systemPrompt');
const marketTools = require('./marketTools');

const client = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// Per-channel conversation history (rolling window)
// Map<channelId, Array<{role, content}>>
const histories = new Map();

const MAX_HISTORY = 20;        // messages to retain per channel
const MAX_TOKENS  = 1024;      // max response tokens
const MAX_TOOL_ITERATIONS = 6; // safety cap on tool-use loop length

// =============================================================================
// MARKET TOOL GUIDANCE — secondary system block, cached separately
// =============================================================================

const MARKET_TOOL_GUIDANCE = `MARKET TOOL ACCESS

You have three tools for fetching live market data on the six pairs The Watch tracks:
HBARUSD, BTCUSD, DOGUSD, SAUCEUSD, GIBUSD, PACKUSD.

  get_ticker     - current price, 24h range, volume, percent change
  get_orderbook  - depth, walls, bid/ask ratio, spread
  get_ohlc       - 6h candles, recent trend

WHEN TO USE
- A community member asks about a current price, recent movement, market depth, support/resistance, who's defending a level, or any kind of "read" on one of the six pairs.
- A general question like "how's BTC?" is a market-read intent — fetch first, opine after.

NEVER SPECULATE WITHOUT THE BOOK
- Don't form a price opinion without checking the data first. If asked "is HBAR a buy?", fetch ticker + orderbook before answering.
- A single ticker is fine for a quick "where's it trading" question. Add orderbook for any structural read (defended levels, walls, depth). Add ohlc only when trend matters.

OUT OF SCOPE PAIRS
- If asked about ETH, SOL, XRP, or any pair we don't watch, decline politely: "Not watching that one — out of my scope. Want a read on HBAR or BTC instead?"
- Don't make up data for untracked pairs.

RATE LIMITS
- Each tool fetch counts against the user's daily limit (4 fetches/day, refreshes midnight CT).
- If a tool returns rate_limit_exceeded, acknowledge it in character: the user's been told. Mention they can check the public dashboard at the IP or read the Bridge Logs in #bridge-log.
- Admins (Duke) are exempt — no limit applies.

EDITORIAL VOICE WHEN READING THE BOOK
- Cite specific numbers from the data. "$616K of top-10 bid depth" is real; "buyers are stacked" is hand-wavy.
- Identify what the book shows, don't predict. "The bid is heavy 4x at this level" not "BTC will go up".
- Hedera ecosystem cluster awareness: HBAR / SAUCE / GIB / PACK tend to move together. If reading one, note the cluster when relevant.
- Thin books (SAUCE / GIB / PACK) deserve honesty: "12 trades in 24 hours isn't a market, it's a whisper."

OUTPUT
- Keep responses to 2-4 short paragraphs unless asked for a deeper read.
- End with the marker framing where it fits naturally — the marker stays on the deck unless the book earns the trade.

PAIR REFERENCE — surface-level facts for orientation only. Use exactly this depth, never deeper.

  HBAR  — Native gas/staking token of Hedera Hashgraph, the council-governed L1 where the Boons live. Fast finality, low fees. Home waters.

  BTC   — The original. Reserve asset, benchmark for all of crypto. Everyone knows; no elaboration needed.

  DOG   — DOG-GO-TO-THE-MOON. Bitcoin Ordinals + Runes meme coin. Hat-iconography community ("DOG Army"). Lives on Bitcoin via Runes — that's why BTC and DOG move-correlate.

  SAUCE — SaucerSwap's governance/utility token. The #1 DEX on Hedera, real DeFi infrastructure. Thin book on Kraken; most volume happens on-chain.

  GIB   — gib.chat. Hedera-native AI agent token. Small cap, thin market.

  PACK  — HashPack. The main Hedera wallet, pushed the HIP-412 metadata standard the Boons use. Thin book on Kraken; utility lives in the wallet, not the secondary market.

RULES FOR PAIR LORE
- When asked "what is X" or similar, give the surface-level fact above and stop. Do not invent details beyond it.
- Factual yes/no answers about scope are fine ("is GIB on Hedera?" → yes; "is DOG on Bitcoin?" → yes).
- NEVER offer: price predictions, "is it a good buy", community size claims, founder commentary, roadmap speculation, sentiment reads beyond what the book shows.
- For deep lore, history, community vibe, or roadmap questions, redirect: "Not really my waters — their own Discord and X will serve you way better than I can."
- The position is: Boons guy who knows his sea. Never an everything-encyclopedia.`;

// =============================================================================
// HISTORY MANAGEMENT
// =============================================================================

function getHistory(channelId) {
  if (!histories.has(channelId)) histories.set(channelId, []);
  return histories.get(channelId);
}

function trimHistory(history) {
  if (history.length > MAX_HISTORY) {
    history.splice(0, history.length - MAX_HISTORY);
  }
}

function clearHistory(channelId) {
  histories.delete(channelId);
}

// =============================================================================
// IMAGE HANDLING
// =============================================================================

const MAX_IMAGE_DIMENSION = 1024;
const MAX_FETCH_BYTES = 100 * 1024 * 1024;

function fetchImageBytes(url, timeoutMs = 12000, maxRedirects = 5) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https:') ? https : http;
    const req = lib.get(url, { timeout: timeoutMs }, (res) => {
      if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
        res.resume();
        if (maxRedirects <= 0) return reject(new Error('too many redirects'));
        return fetchImageBytes(res.headers.location, timeoutMs, maxRedirects - 1).then(resolve).catch(reject);
      }
      if (res.statusCode !== 200) {
        res.resume();
        return reject(new Error(`HTTP ${res.statusCode}`));
      }
      const chunks = [];
      let total = 0;
      res.on('data', c => {
        total += c.length;
        if (total > MAX_FETCH_BYTES) {
          req.destroy();
          return reject(new Error('image exceeds fetch cap'));
        }
        chunks.push(c);
      });
      res.on('end', () => resolve(Buffer.concat(chunks)));
    });
    req.on('error', reject);
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
  });
}

async function prepareImageBlock(url) {
  try {
    const buf = await fetchImageBytes(url);
    const resized = await sharp(buf)
      .rotate()
      .resize(MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION, {
        fit: 'inside',
        withoutEnlargement: true,
      })
      .jpeg({ quality: 88, mozjpeg: true })
      .toBuffer();
    return {
      type: 'image',
      source: {
        type: 'base64',
        media_type: 'image/jpeg',
        data: resized.toString('base64'),
      },
    };
  } catch (e) {
    console.error(`[claude] image prep failed for ${url.slice(0, 80)}: ${e.message}`);
    return null;
  }
}

// =============================================================================
// USAGE LOGGING
// =============================================================================

function logUsage(info) {
  try {
    const line = JSON.stringify({ ts: new Date().toISOString(), ...info }) + '\n';
    fs.appendFileSync('/home/capt-crawl/usage-log.jsonl', line);
  } catch (e) {
    console.error('[logUsage] WRITE FAILED:', e.message);
  }
}

// Accumulate token counts across iterations of the tool-use loop
function accumulate(totals, usage) {
  if (!usage) return;
  totals.input_tokens               += (usage.input_tokens || 0);
  totals.cache_creation_input_tokens += (usage.cache_creation_input_tokens || 0);
  totals.cache_read_input_tokens     += (usage.cache_read_input_tokens || 0);
  totals.output_tokens              += (usage.output_tokens || 0);
}

function computeCostUsd(totals) {
  // Sonnet 4.6 pricing per 1M tokens: $3 input, $15 output, $3.75 cache create, $0.30 cache read
  return (
    (totals.input_tokens               * 3.00 +
     totals.cache_creation_input_tokens * 3.75 +
     totals.cache_read_input_tokens     * 0.30 +
     totals.output_tokens              * 15.00) / 1_000_000
  );
}

// =============================================================================
// MAIN ASK FUNCTION — supports tool use
// =============================================================================

/**
 * Ask Claude a question with optional live context + image URLs + user identity.
 *
 * options: { userId, userName } — required for rate-limit-gated market tool use
 */
async function ask(channelId, userMessage, liveContext = '', imageUrls = [], options = {}) {
  const history = getHistory(channelId);
  const userId   = options.userId   || 'unknown';
  const userName = options.userName || 'unknown';

  const fullUserText = liveContext ? `${userMessage}${liveContext}` : userMessage;
  const hasImages = Array.isArray(imageUrls) && imageUrls.length > 0;

  // Build request-time user content (real image blocks if present)
  let requestUserContent;
  if (hasImages) {
    const preparedBlocks = [];
    for (const url of imageUrls) {
      const block = await prepareImageBlock(url);
      if (block) preparedBlocks.push(block);
    }
    if (preparedBlocks.length === 0) {
      requestUserContent = '[user shared an image but it was too large to process — ask them to resend smaller] ' + fullUserText;
    } else {
      requestUserContent = [...preparedBlocks, { type: 'text', text: fullUserText }];
    }
  } else {
    requestUserContent = fullUserText;
  }

  // History-safe user content (text markers replace image URLs since Discord CDN expires)
  const historyUserContent = hasImages
    ? `${imageUrls.length === 1 ? '[user shared an image]' : `[user shared ${imageUrls.length} images]`} ${fullUserText}`.trim()
    : fullUserText;

  // Build messages array — will grow as the tool-use loop iterates
  let messages = [
    ...history,
    { role: 'user', content: requestUserContent },
  ];

  const usageTotals = {
    input_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    output_tokens: 0,
  };
  let toolCallsExecuted = 0;
  let finalReply = '';

  try {
    for (let iter = 0; iter < MAX_TOOL_ITERATIONS; iter++) {
      const response = await client.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: MAX_TOKENS,
        system: [
          {
            type: 'text',
            text: SYSTEM_PROMPT,
            cache_control: { type: 'ephemeral' },
          },
          {
            type: 'text',
            text: MARKET_TOOL_GUIDANCE,
            cache_control: { type: 'ephemeral' },
          },
        ],
        tools: marketTools.getToolDefinitions(),
        messages,
      });

      accumulate(usageTotals, response.usage);

      // If the model is done, extract the text reply and break.
      if (response.stop_reason !== 'tool_use') {
        finalReply = response.content
          .filter(b => b.type === 'text')
          .map(b => b.text)
          .join('');
        break;
      }

      // Tool use — append assistant turn, execute tools, append results.
      messages.push({ role: 'assistant', content: response.content });

      const toolResultBlocks = [];
      for (const block of response.content) {
        if (block.type !== 'tool_use') continue;
        toolCallsExecuted++;

        const result = await marketTools.executeToolCall(
          block.name,
          block.input || {},
          userId,
          userName,
        );

        toolResultBlocks.push({
          type: 'tool_result',
          tool_use_id: block.id,
          content: JSON.stringify(result),
        });
      }

      if (toolResultBlocks.length === 0) {
        // Safety: stop_reason said tool_use but no tool_use blocks present.
        // Extract whatever text we have and bail.
        finalReply = response.content
          .filter(b => b.type === 'text')
          .map(b => b.text)
          .join('') || 'Ran into some rough waters fetching that data.';
        break;
      }

      messages.push({ role: 'user', content: toolResultBlocks });
    }

    if (!finalReply) {
      // Hit MAX_TOOL_ITERATIONS without converging
      finalReply = "Books are noisy right now — too much to chase in one read. Try a more specific question and I'll dig in.";
      console.warn(`[claude] tool-use loop hit MAX_TOOL_ITERATIONS for user ${userId}`);
    }

    // Log accumulated usage once
    try {
      logUsage({
        channelId,
        userId,
        userName,
        imageCount: imageUrls.length,
        toolCalls: toolCallsExecuted,
        input_tokens:                 usageTotals.input_tokens,
        cache_creation_input_tokens:  usageTotals.cache_creation_input_tokens,
        cache_read_input_tokens:      usageTotals.cache_read_input_tokens,
        output_tokens:                usageTotals.output_tokens,
        cost_usd: parseFloat(computeCostUsd(usageTotals).toFixed(6)),
      });
    } catch (e) {
      console.error('[logUsage ERR]', e.message);
    }

    // Persist history with safe markers (initial user msg + final assistant text only).
    // Tool use/result blocks are deliberately NOT persisted — they'd bloat the rolling
    // window with stale data and the model can re-fetch fresh data if the topic recurs.
    history.push({ role: 'user', content: historyUserContent });
    history.push({ role: 'assistant', content: finalReply });
    trimHistory(history);

    return finalReply;
  } catch (e) {
    console.error('[Claude] API error:', e.message);
    throw e;
  }
}

module.exports = { ask, clearHistory };
