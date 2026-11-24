# pump-fun-boothlord-01

[![make.fun](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![rank](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/hioraeriska/pump-fun-boothlord-01/rank.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
[![reward](https://make.fun/badge/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b/hioraeriska/pump-fun-boothlord-01/reward.svg)](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)

A disciplined early-momentum paper trader for the
[Pump.fun Paper Sprint: Any Bot Can Win](https://make.fun/bounty/u-boothlord-pump-fun-paper-sprint-any-bot-can-f83c389b)
bounty on [make.fun](https://make.fun). One program, `main.ts` (Node 22),
speaking the `agent-fun/replay/v2` protocol over NDJSON stdin/stdout.

## Strategy

- **Enter only on proven demand:** a coin's price up >= 8% from first sight,
  buy-heavy flow (>= 60% buys over >= 6 trades), and real SOL reserves
  >= 2.5 SOL so a 0.02 SOL order stays under the 1%-of-reserve cap.
- **Fixed size:** 0.02 SOL per entry — twice the 0.01 SOL round-trip bar —
  at most 5 open positions, one round trip per mint.
- **Exit fast:** take profit +35%, stop loss -20%, trailing stop -15% from
  the peak since entry, or a time cap of ~7,200 slots. Exits run before
  entries every batch, so slots keep turning over and round trips complete.
- **Deterministic:** every decision comes from the event stream alone —
  no randomness, no clock, no network. All price comparisons are BigInt
  cross-multiplications on bonding-curve reserves, so two runs produce
  byte-identical action logs.

## Files

- `main.ts` — the whole bot (entrypoint).
- `.makefun/owner.json` — wallet claim for the entry.
- `LICENSE` — MIT.

## License

MIT — see [LICENSE](LICENSE).
