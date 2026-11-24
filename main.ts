// pump-fun-boothlord-01 — disciplined early-momentum paper trader.
//
// Speaks agent-fun/replay/v2 over NDJSON: the harness sends a hello, then
// one batch per slot window, then done. We answer every batch with one
// reply echoing its seq and carrying our orders for that batch.
//
// Idea, in one line: buy a coin only after it has proven demand (price up
// >=8% from first sight, buy-heavy flow, real reserves >= 2.5 SOL so a
// 0.02 SOL order stays under the 1%-of-reserve cap), then exit fast on a
// take-profit, a stop-loss, a trailing stop, or a time cap. Small fixed
// size, at most 5 open positions, one round trip per mint. Everything is
// decided from the event stream alone: no randomness, no clock, no network,
// so two runs produce byte-identical action logs.
//
// All price comparisons use BigInt cross-multiplication on the bonding
// curve reserves (price = virtual SOL reserves / virtual token reserves),
// so no floating point ever touches a decision.

import { createInterface } from "node:readline";

// ---- Tunables (lamports unless noted) ------------------------------------
const BUY_LAMPORTS = 20_000_000n; // 0.02 SOL per entry: 2x the 0.01 SOL bar
const MIN_REAL_RESERVES = 2_500_000_000n; // 2.5 SOL: 0.02 SOL is 0.8% of it
const MAX_SLIPPAGE_BPS = 500; // 5%
const ENTRY_RISE_NUM = 108n; // enter at +8% vs first-seen price
const ENTRY_RISE_DEN = 100n;
const MIN_TRADES_TO_ENTER = 6; // need a little flow history first
const MIN_BUY_SHARE_NUM = 3n; // at least 60% of observed trades are buys
const MIN_BUY_SHARE_DEN = 5n;
const TAKE_PROFIT_NUM = 135n; // exit at +35%
const STOP_LOSS_NUM = 80n; // exit at -20%
const TRAIL_NUM = 85n; // exit at -15% from the peak since entry
const MAX_HOLD_SLOTS = 7200; // ~48 min: free the slot, bank the round trip
const MAX_BUYS_PER_BATCH = 3;

// ---- Types -----------------------------------------------------------------
type Price = { n: bigint; d: bigint }; // n/d = lamports per token base unit

type Coin = {
  first: Price | null; // price at first sight (create or first trade)
  cur: Price | null; // latest price from marks / events
  realSol: bigint; // latest real SOL reserves
  trades: number;
  buys: number;
  seenSlot: number;
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
    c = { first: null, cur: null, realSol: 0n, trades: 0, buys: 0, seenSlot: slot };
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

  // 1) Exits first: free slots before opening new ones.
  const mints = [...positions.keys()].sort();
  for (const mint of mints) {
    const pos = positions.get(mint);
    if (!pos) continue;
    const c = coins.get(mint);
    const cur = c?.cur;
    if (!cur) continue;
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
          max_slippage_bps: MAX_SLIPPAGE_BPS,
        })
      ) {
        traded.add(mint);
      }
    }
  }

  // 2) Entries: proven demand, enough real reserves, a free slot, cash.
  const openCount = positions.size + pendingBuys.size;
  let buysPlaced = 0;
  if (openCount < maxOpen && cash >= BUY_LAMPORTS + 1_000_000n) {
    const candidates = [...coins.keys()].sort();
    for (const mint of candidates) {
      if (buysPlaced >= MAX_BUYS_PER_BATCH) break;
      if (positions.size + pendingBuys.size >= maxOpen) break;
      if (cash < BUY_LAMPORTS + 1_000_000n) break;
      if (positions.has(mint) || traded.has(mint)) continue;
      const c = coins.get(mint);
      if (!c || !c.first || !c.cur) continue;
      if (c.trades < MIN_TRADES_TO_ENTER) continue;
      if (c.realSol < MIN_REAL_RESERVES) continue;
      // Buy-heavy flow: buys / trades >= 60%.
      if (BigInt(c.buys) * MIN_BUY_SHARE_DEN < BigInt(c.trades) * MIN_BUY_SHARE_NUM) continue;
      // Momentum: current price >= first price * 1.08.
      if (!priceAtLeast(c.cur, c.first, ENTRY_RISE_NUM, ENTRY_RISE_DEN)) continue;
      if (
        push({
          side: "buy",
          client_id: nextClientId("b"),
          mint,
          quote_lamports: BUY_LAMPORTS.toString(),
          max_slippage_bps: MAX_SLIPPAGE_BPS,
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

  // Marks: freshest reserves per mint this batch.
  if (Array.isArray(msg.marks)) {
    for (const m of msg.marks) {
      if (typeof m.mint !== "string") continue;
      const c = coinFor(m.mint, slot);
      const n = BigInt(m.virtual_quote_reserves ?? "0");
      const d = BigInt(m.virtual_token_reserves ?? "0");
      if (d > 0n) {
        c.cur = { n, d };
        if (!c.first) c.first = { n, d };
      }
      if (typeof m.real_quote_reserves === "string") {
        const r = BigInt(m.real_quote_reserves);
        if (r > c.realSol) c.realSol = r;
      }
    }
  }

  // Events: flow stats, first-seen prices, real reserves.
  if (Array.isArray(msg.events)) {
    for (const e of msg.events) {
      if (e.event === "CreateEvent" && typeof e.mint === "string") {
        const c = coinFor(e.mint, slot);
        const n = BigInt(e.virtual_sol_reserves ?? e.virtual_quote_reserves ?? "0");
        const d = BigInt(e.virtual_token_reserves ?? "0");
        if (d > 0n && !c.first) c.first = { n, d };
        if (d > 0n) c.cur = { n, d };
      } else if (e.event === "TradeEvent" && typeof e.mint === "string") {
        const c = coinFor(e.mint, slot);
        const n = BigInt(e.virtual_sol_reserves ?? "0");
        const d = BigInt(e.virtual_token_reserves ?? "0");
        if (d > 0n) {
          if (!c.first) c.first = { n, d };
          c.cur = { n, d };
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

  // Fills: confirm buys (fill-implied entry price), clear sells / rejects.
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
        if (typeof f.mint === "string") positions.delete(f.mint);
      } else if (f.status === "rejected" || f.status === "failed") {
        if (typeof f.client_id === "string") pendingBuys.delete(f.client_id);
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
