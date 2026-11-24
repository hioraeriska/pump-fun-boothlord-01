// pump-fun-boothlord-01 — disciplined early-momentum paper trader.
//
// Speaks agent-fun/replay/v2 over NDJSON: the harness sends a hello, then
// one batch per slot window, then done. We answer every batch with one
// reply echoing its seq and carrying our orders for that batch.
//
// Idea, in one line: buy a coin only after it has proven demand (price up
// >=5% from first sight, buy-heavy flow, real reserves >= 1.5 SOL, sized at
// 0.8% of real reserves so the order stays under the 1%-of-reserve cap),
// then exit fast on a take-profit, a stop-loss, a trailing stop, or a time
// cap. At most 5 open positions, one round trip per mint, and a mint
// whose buy fails for a permanent reason is never retried (a transient
// slippage reject only cools it down briefly). Buys tolerate 10%
// slippage, sells 20%, because the harness rejects an order whose fill
// moved past its slippage band in the 2-slot latency — and a rejected
// exit during a crash is how a -8% trailing stop becomes a -50% loss.
//
// All price comparisons use BigInt cross-multiplication on the bonding
// curve reserves (price = virtual SOL reserves / virtual token reserves),
// so no floating point ever touches a decision.

import { createInterface } from "node:readline";

// ---- Tunables (lamports unless noted) ------------------------------------
// Size scales with the coin's real reserves (0.8% of them) so the order
// stays well under the 1%-of-reserve cap, floored above the 0.01 SOL bar
// and capped to keep one loser small.
const MIN_BUY_LAMPORTS = 12_000_000n; // 0.012 SOL
const MAX_BUY_LAMPORTS = 100_000_000n; // 0.10 SOL
const MIN_REAL_RESERVES = 1_500_000_000n; // 1.5 SOL
const MAX_SLIPPAGE_BPS_BUY = 1000; // 10%: fills must survive fast surges
const MAX_SLIPPAGE_BPS_SELL = 2000; // 20%: exits must fill through crashes
const ENTRY_RISE_NUM = 105n; // enter at +5% vs first-seen price
const ENTRY_RISE_DEN = 100n;
const MIN_TRADES_TO_ENTER = 5; // need a little flow history first
const MIN_BUY_SHARE_NUM = 3n; // at least 60% of observed trades are buys
const MIN_BUY_SHARE_DEN = 5n;
const TAKE_PROFIT_NUM = 120n; // exit at +20%
const STOP_LOSS_NUM = 90n; // exit at -10%
const TRAIL_NUM = 92n; // exit at -8% from the peak since entry
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

  // 2) Entries: proven demand, enough real reserves, a free slot, cash.
  // Size is 0.8% of the coin's real reserves, floored above the bar.
  const openCount = positions.size + pendingBuys.size;
  let buysPlaced = 0;
  if (openCount < maxOpen && cash >= MIN_BUY_LAMPORTS + 1_000_000n) {
    const candidates = [...coins.keys()].sort();
    for (const mint of candidates) {
      if (buysPlaced >= MAX_BUYS_PER_BATCH) break;
      if (positions.size + pendingBuys.size >= maxOpen) break;
      if (positions.has(mint) || traded.has(mint) || blocked.has(mint)) continue;
      const cool = blockedUntil.get(mint);
      if (cool !== undefined && currentSlot < cool) continue;
      const c = coins.get(mint);
      if (!c || !c.first || !c.cur) continue;
      if (c.trades < MIN_TRADES_TO_ENTER) continue;
      if (c.realSol < MIN_REAL_RESERVES) continue;
      // Buy-heavy flow: buys / trades >= 60%.
      if (BigInt(c.buys) * MIN_BUY_SHARE_DEN < BigInt(c.trades) * MIN_BUY_SHARE_NUM) continue;
      // Momentum: current price >= first price * 1.05.
      if (!priceAtLeast(c.cur, c.first, ENTRY_RISE_NUM, ENTRY_RISE_DEN)) continue;
      const sized = (c.realSol * 80n) / 10000n; // 0.8% of real reserves
      const quote = sized < MIN_BUY_LAMPORTS ? MIN_BUY_LAMPORTS : sized > MAX_BUY_LAMPORTS ? MAX_BUY_LAMPORTS : sized;
      if (cash < quote + 1_000_000n) continue;
      if (
        push({
          side: "buy",
          client_id: nextClientId("b"),
          mint,
          quote_lamports: quote.toString(),
          max_slippage_bps: MAX_SLIPPAGE_BPS_BUY,
        })
      ) {
        pendingBuys.set(orders[orders.length - 1].client_id, mint);
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
          const r = BigInt(e.real_sol_reserves);
          if (r > c.realSol) c.realSol = r;
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
        const r = BigInt(m.real_quote_reserves);
        if (r > c.realSol) c.realSol = r;
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
        if (mint && typeof f.mint === "string") {
          const tokens = BigInt(f.token_amount ?? "0");
          const quote = BigInt(f.quote_lamports ?? "0");
          if (tokens > 0n && quote > 0n) {
            const entry: Price = { n: quote, d: tokens };
            positions.set(mint, { tokens, entry, peak: entry, entrySlot: slot });
          }
        }
      } else if (f.status === "filled" && f.side === "sell") {
        if (typeof f.mint === "string") {
          positions.delete(f.mint);
          pendingSells.delete(f.mint);
        }
      } else if (f.status === "rejected" || f.status === "failed") {
        if (typeof f.client_id === "string") {
          const mint = pendingBuys.get(f.client_id);
          if (mint) {
            // Permanent reasons: the mint is not tradeable at our size.
            // Transient ones (max_slippage, insufficient_cash, failed):
            // cool down briefly, then the mint may qualify again.
            if (f.reason === "not_in_universe" || f.reason === "token_complete" || f.reason === "max_order_size" || f.reason === "zero_amount") {
              blocked.add(mint);
            } else {
              blockedUntil.set(mint, slot + 100);
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
