// pump-fun-boothlord-01 — disciplined early-buyer paper trader.
//
// Speaks agent-fun/replay/v2 over NDJSON: the harness sends a hello, then
// one batch per slot window, then done. We answer every batch with one
// reply echoing its seq and carrying our orders for that batch.
//
// Idea, in one line: buy a coin as soon as it is big enough to take a
// 0.02 SOL order (real reserves >= 2.5 SOL, so the order is 0.8% of
// reserves, under the 1% cap) — no momentum filter, because on this data
// waiting for a rise buys the top and loses, while the early coins are
// where the surge is — then exit fast on a take-profit, a stop-loss, a
// trailing stop, or a time cap. At most 5 open positions, one round trip
// per mint, and never two buys on the same mint at once: a second buy
// would overwrite the tracked position while the engine keeps both lots,
// stranding tokens no exit can ever sell (that bug cost v4 its whole run:
// 19 positions stuck open, 184k max_open_positions rejections). A mint
// whose buy fails for a permanent reason is never retried (a transient
// slippage reject only cools it down briefly). Buys tolerate 10%
// slippage, sells 20%, because the harness rejects an order whose fill
// moved past its slippage band in the 2-slot latency.
//
// All price comparisons use BigInt cross-multiplication on the bonding
// curve reserves (price = virtual SOL reserves / virtual token reserves),
// so no floating point ever touches a decision.

import { createInterface } from "node:readline";

// ---- Tunables (lamports unless noted) ------------------------------------
// Fixed small size: 0.02 SOL per entry, twice the 0.01 SOL round-trip bar.
// It needs real reserves >= 2.5 SOL (0.8% of reserves, under the 1% cap).
// Shape (validated on the dev window: momentum entries lose, early entries
// win): buy a coin as soon as it is big enough to take our order, with no
// momentum filter — most coins pump hardest in their first minutes, and
// waiting for a +5% confirmation systematically buys the top. Exits do the
// work: take profit +35%, stop loss -12%, trailing stop -10% from the peak,
// or a ~13-minute time cap so slots keep turning over.
const BUY_LAMPORTS = 20_000_000n; // 0.02 SOL per entry
const MIN_REAL_RESERVES = 2_500_000_000n; // 2.5 SOL
const MIN_TRADES_TO_ENTER = 2; // need the create + a couple of trades seen
const MAX_SLIPPAGE_BPS_BUY = 150; // 1.5%: buys must fill near the decision
// price. The harness rejects a fill worse than the band, and rejections
// are free — so a tight band is a filter, not a cost: it refuses to chase
// a burst that ran away in the 2-slot latency, and only fills when the
// price is calm or dipping into the order. (v5 forensics: fills ran ~5-10%
// above decision on surging coins, so 88% of entries never peaked +5%
// above their fill price. Wide buy slippage was the adverse selection.)
const MAX_SLIPPAGE_BPS_SELL = 2000; // 20%: exits must fill through crashes
const TAKE_PROFIT_NUM = 135n; // exit at +35%
const STOP_LOSS_NUM = 88n; // exit at -12%
const TRAIL_NUM = 90n; // exit at -10% from the peak since entry
const MAX_HOLD_SLOTS = 3600; // ~13 min: free the slot, bank the round trip
const MAX_BUYS_PER_BATCH = 5;

// ---- Types -----------------------------------------------------------------
type Price = { n: bigint; d: bigint }; // n/d = lamports per token base unit

type Coin = {
  first: Price | null; // price at first sight (create or first trade)
  cur: Price | null; // latest price from marks / events
  curSlot: number; // slot of the data behind cur (marks win over events)
  realSol: bigint; // latest real SOL reserves
  trades: number;
  buys: number;
  seenSlot: number;
  inMarks: boolean; // seen in a marks array: the harness tracks this mint
};

type Position = {
  tokens: bigint;
  entry: Price; // fill-implied entry price
  peak: Price; // highest price seen since entry
  entrySlot: number;
};

type Order =
  | {
      side: "buy";
      client_id: string;
      mint: string;
      quote_lamports: string;
      max_slippage_bps: number;
    }
  | {
      side: "sell";
      client_id: string;
      mint: string;
      token_amount: string;
      max_slippage_bps: number;
    };

// ---- Small integer helpers -------------------------------------------------
// Is price a/b >= k (k given as num/den)? Cross-multiply, no floats.
function priceAtLeast(p: Price, ref: Price, num: bigint, den: bigint): boolean {
  return p.n * ref.d * num >= ref.n * p.d * den;
}
// Is price a/b <= k?
function priceAtMost(p: Price, ref: Price, num: bigint, den: bigint): boolean {
  return p.n * ref.d * num <= ref.n * p.d * den;
}
function priceGt(a: Price, b: Price): boolean {
  return a.n * b.d > b.n * a.d;
}

// ---- State -----------------------------------------------------------------
const coins = new Map<string, Coin>();
const positions = new Map<string, Position>();
const pendingBuys = new Map<string, string>(); // client_id -> mint
const pendingBuyMints = new Set<string>(); // mints with a buy already in flight
const pendingSells = new Set<string>(); // mints with a sell already in flight
const blocked = new Set<string>(); // mints whose buy failed permanently: never retry
const blockedUntil = new Map<string, number>(); // mint -> slot: cooldown after a transient buy reject
const traded = new Set<string>(); // mints already round-tripped: one trip each
const accountTokens = new Map<string, bigint>(); // mint -> tokens per account
let cash = 0n;
let maxOpen = 5;
let maxOrdersPerBatch = 16;
let orderCounter = 0;
let currentSlot = 0;

function coinFor(mint: string, slot: number): Coin {
  let c = coins.get(mint);
  if (!c) {
    c = { first: null, cur: null, curSlot: -1, realSol: 0n, trades: 0, buys: 0, seenSlot: slot, inMarks: false };
    coins.set(mint, c);
  }
  return c;
}

function nextClientId(kind: string): string {
  orderCounter += 1;
  return `${kind}-${orderCounter}`;
}

// ---- Per-batch decision ----------------------------------------------------
function decide(seq: number): { type: "reply"; seq: number; orders: Order[] } {
  const orders: Order[] = [];
  const push = (o: Order): boolean => {
    if (orders.length >= maxOrdersPerBatch) return false;
    orders.push(o);
    return true;
  };

  // 1) Exits first: free slots before opening new ones. Never stack a
  // second sell on a mint whose sell is already in flight.
  const mints = [...positions.keys()].sort();
  for (const mint of mints) {
    const pos = positions.get(mint);
    if (!pos) continue;
    const c = coins.get(mint);
    const cur = c?.cur;
    if (!cur) continue;
    if (pendingSells.has(mint)) continue;
    const tokens = accountTokens.get(mint) ?? pos.tokens;
    if (tokens <= 0n) {
      positions.delete(mint);
      continue;
    }
    if (priceGt(cur, pos.peak)) pos.peak = cur;
    const takeProfit = priceAtLeast(cur, pos.entry, TAKE_PROFIT_NUM, 100n);
    const stopLoss = priceAtMost(cur, pos.entry, STOP_LOSS_NUM, 100n);
    const trailing = priceAtMost(cur, pos.peak, TRAIL_NUM, 100n);
    const tooOld = currentSlot - pos.entrySlot >= MAX_HOLD_SLOTS;
    if (takeProfit || stopLoss || trailing || tooOld) {
      if (
        push({
          side: "sell",
          client_id: nextClientId("s"),
          mint,
          token_amount: tokens.toString(),
          max_slippage_bps: MAX_SLIPPAGE_BPS_SELL,
        })
      ) {
        pendingSells.add(mint);
        traded.add(mint);
      }
    }
  }

  // 2) Entries: buy early — as soon as the coin is big enough to take a
  // 0.02 SOL order at 0.8% of its real reserves. No momentum filter: on
  // this data, waiting for a rise buys the top and loses; the early coins
  // are where the surge is. One trip per mint, a free slot, and cash.
  const openCount = positions.size + pendingBuys.size;
  let buysPlaced = 0;
  if (openCount < maxOpen && cash >= BUY_LAMPORTS + 1_000_000n) {
    const candidates = [...coins.keys()].sort();
    for (const mint of candidates) {
      if (buysPlaced >= MAX_BUYS_PER_BATCH) break;
      if (positions.size + pendingBuys.size >= maxOpen) break;
      if (positions.has(mint) || pendingBuyMints.has(mint) || traded.has(mint) || blocked.has(mint)) continue;
      const cool = blockedUntil.get(mint);
      if (cool !== undefined && currentSlot < cool) continue;
      const c = coins.get(mint);
      if (!c || !c.first || !c.cur) continue;
      if (c.trades < MIN_TRADES_TO_ENTER) continue;
      if (c.realSol < MIN_REAL_RESERVES) continue;
      if (
        push({
          side: "buy",
          client_id: nextClientId("b"),
          mint,
          quote_lamports: BUY_LAMPORTS.toString(),
          max_slippage_bps: MAX_SLIPPAGE_BPS_BUY,
        })
      ) {
        pendingBuys.set(orders[orders.length - 1].client_id, mint);
        pendingBuyMints.add(mint);
        buysPlaced += 1;
      }
    }
  }

  return { type: "reply", seq, orders };
}

// ---- Message handling ------------------------------------------------------
function onHello(msg: any): void {
  if (typeof msg.capital?.lamports === "string") cash = BigInt(msg.capital.lamports);
  if (typeof msg.caps?.max_open_positions === "number") maxOpen = msg.caps.max_open_positions;
  if (typeof msg.caps?.max_orders_per_batch === "number") {
    maxOrdersPerBatch = msg.caps.max_orders_per_batch;
  }
}

function onBatch(msg: any): { type: "reply"; seq: number; orders: Order[] } {
  const slot = typeof msg.slot === "number" ? msg.slot : 0;
  currentSlot = slot;
  const seq = typeof msg.seq === "number" ? msg.seq : 0;

  // Events first: flow stats, first-seen prices, real reserves.
  if (Array.isArray(msg.events)) {
    for (const e of msg.events) {
      const eslot = typeof e.slot === "number" ? e.slot : slot;
      if (e.event === "CreateEvent" && typeof e.mint === "string") {
        const c = coinFor(e.mint, slot);
        const n = BigInt(e.virtual_sol_reserves ?? e.virtual_quote_reserves ?? "0");
        const d = BigInt(e.virtual_token_reserves ?? "0");
        if (d > 0n && !c.first) c.first = { n, d };
        if (d > 0n && eslot >= c.curSlot) {
          c.cur = { n, d };
          c.curSlot = eslot;
        }
      } else if (e.event === "TradeEvent" && typeof e.mint === "string") {
        const c = coinFor(e.mint, slot);
        const n = BigInt(e.virtual_sol_reserves ?? "0");
        const d = BigInt(e.virtual_token_reserves ?? "0");
        if (d > 0n) {
          if (!c.first) c.first = { n, d };
          if (eslot >= c.curSlot) {
            c.cur = { n, d };
            c.curSlot = eslot;
          }
        }
        if (typeof e.real_sol_reserves === "string") {
          c.realSol = BigInt(e.real_sol_reserves);
        }
        c.trades += 1;
        if (e.is_buy === true) c.buys += 1;
      }
    }
  }

  // Marks last: they are the freshest reserves per mint this batch.
  if (Array.isArray(msg.marks)) {
    for (const m of msg.marks) {
      if (typeof m.mint !== "string") continue;
      const c = coinFor(m.mint, slot);
      c.inMarks = true;
      const mslot = typeof m.slot === "number" ? m.slot : slot;
      const n = BigInt(m.virtual_quote_reserves ?? "0");
      const d = BigInt(m.virtual_token_reserves ?? "0");
      if (d > 0n && mslot >= c.curSlot) {
        c.cur = { n, d };
        c.curSlot = mslot;
        if (!c.first) c.first = { n, d };
      }
      if (typeof m.real_quote_reserves === "string") {
        c.realSol = BigInt(m.real_quote_reserves);
      }
    }
  }

  // Fills: confirm buys (fill-implied entry price), clear sells / rejects.
  // A rejected buy blocks that mint for good: retrying it every batch
  // only spams the log and the coin is not tradeable at our size anyway.
  if (Array.isArray(msg.fills)) {
    for (const f of msg.fills) {
      if (f.status === "filled" && f.side === "buy" && typeof f.client_id === "string") {
        const mint = pendingBuys.get(f.client_id);
        pendingBuys.delete(f.client_id);
        if (mint) pendingBuyMints.delete(mint);
        if (mint && typeof f.mint === "string") {
          const tokens = BigInt(f.token_amount ?? "0");
          const quote = BigInt(f.quote_lamports ?? "0");
          if (tokens > 0n && quote > 0n) {
            const entry: Price = { n: quote, d: tokens };
            const existing = positions.get(mint);
            if (existing) {
              // Top-up, never overwrite: keep the first entry, add tokens.
              existing.tokens += tokens;
            } else {
              positions.set(mint, { tokens, entry, peak: entry, entrySlot: slot });
            }
          }
        }
      } else if (f.status === "filled" && f.side === "sell") {
        if (typeof f.mint === "string") {
          pendingSells.delete(f.mint);
          // Do not delete the position here: the account snapshot below
          // is the source of truth. If tokens remain, the position stays.
          const pos = positions.get(f.mint);
          if (pos) {
            const sold = BigInt(f.token_amount ?? "0");
            pos.tokens -= sold;
            if (pos.tokens <= 0n) positions.delete(f.mint);
          }
        }
      } else if (f.status === "rejected" || f.status === "failed") {
        if (typeof f.client_id === "string") {
          const mint = pendingBuys.get(f.client_id);
          if (mint) {
            pendingBuyMints.delete(mint);
            // Permanent reasons: the mint is not tradeable at our size.
            // Transient ones (max_slippage, insufficient_cash, failed):
            // cool down briefly, then the mint may qualify again.
            if (f.reason === "not_in_universe" || f.reason === "token_complete" || f.reason === "max_order_size" || f.reason === "zero_amount") {
              blocked.add(mint);
            } else {
              blockedUntil.set(mint, slot + 200);
            }
          }
          pendingBuys.delete(f.client_id);
        }
        if (f.side === "sell" && typeof f.mint === "string") {
          // Sell rejected (e.g. amount raced): allow one retry next batch.
          pendingSells.delete(f.mint);
        }
      }
    }
  }

  // Account: cash and token balances are the source of truth.
  if (msg.account) {
    if (typeof msg.account.cash_lamports === "string") {
      cash = BigInt(msg.account.cash_lamports);
    }
    accountTokens.clear();
    if (Array.isArray(msg.account.positions)) {
      for (const p of msg.account.positions) {
        if (typeof p.mint !== "string") continue;
        const tokens = BigInt(p.token_amount ?? "0");
        accountTokens.set(p.mint, tokens);
        // Keep our tracked position size in sync with the account.
        const pos = positions.get(p.mint);
        if (pos && tokens !== pos.tokens) pos.tokens = tokens;
        if (pos && tokens <= 0n) positions.delete(p.mint);
      }
    }
    // A position the account no longer holds is gone.
    for (const mint of [...positions.keys()]) {
      if (!accountTokens.has(mint) || (accountTokens.get(mint) ?? 0n) <= 0n) {
        positions.delete(mint);
      }
    }
  }

  return decide(seq);
}

// ---- Main loop: one reply per batch, echoing its seq -----------------------
const rl = createInterface({ input: process.stdin, terminal: false });
rl.on("line", (line: string) => {
  const text = line.trim();
  if (!text) return;
  let msg: any;
  try {
    msg = JSON.parse(text);
  } catch {
    return;
  }
  if (msg.type === "hello") {
    onHello(msg);
  } else if (msg.type === "batch") {
    const reply = onBatch(msg);
    process.stdout.write(JSON.stringify(reply) + "\n");
  } else if (msg.type === "done") {
    rl.close();
    process.exit(0);
  }
});
