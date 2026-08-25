# Binance Spot triangular arbitrage

Fail-closed, fee-aware triangular-arbitrage scanner and sequential executor.
Live trading is disabled unless both `ARB_DRY_RUN_BINANCE=false` and
`BINANCE_LIVE_ACK=I_ACCEPT_LIVE_TRADING` are configured.

The architecture reuses the durable execution policy from `rev_lazy_arb` and
the sibling Kraken triangle service, with Binance-native HMAC signing,
`exchangeInfo` filters, `quoteOrderQty` market buys, deterministic client order
IDs, and exact fill-commission accounting.

## Safety and execution

- The checked-in default is dry-run. Live `run` additionally requires
  `BINANCE_LIVE_ACK=I_ACCEPT_LIVE_TRADING`; `scan-once` can never trade.
- An all-market WebSocket cache retains one bid/ask tuple per symbol in memory.
  Raw ticks are never written. REST depth is requested only for profitable
  ticker candidates, and all three books are walked before authorization.
- Binance's account taker tier is used conservatively for screening. Each live
  route is revalidated using non-executing `order/test` fee computation and
  fresh depth immediately before leg 1.
- Every leg tries fresh top-of-book `LIMIT/FOK`, then `LIMIT/IOC` for any
  confirmed remainder, then `MARKET` for the still-executable remainder. Each
  attempt is fsynced before placement. A transport/5xx timeout
  is resolved using its deterministic `newClientOrderId`; it is never blindly
  retried. Confirmed intermediate inventory is flattened to the start asset on
  ordinary failures. Ambiguous exposure stops for operator recovery.
- Opportunity sizing matches lazy arb: the cap is the full free start balance,
  while the conservative probe grid starts at half of that cap and halves down
  to the configured floor. Immediately before leg 1, fresh balances and all
  three books rerun the complete grid and may resize the opportunity.
- Completed state records are capped at 2,000 files and 30 days by default.
  Docker logs rotate at 30 MB for the strategy and 10 MB for Telegram.
- Healthy operation emits a `scan heartbeat` every 10 seconds by default,
  including scan count, fresh tickers, triangle/candidate counts, best observed
  signal, decision, and funded start balances. Candidate, depth, execution,
  fill, and recovery decisions are logged as they occur. Configure the cadence
  with `ARB_SCAN_LOG_INTERVAL_S_BINANCE`.

## Commands

```bash
docker compose build
docker compose run --rm binarb python -m binarb probe
docker compose run --rm binarb python -m binarb probe --validate-order
docker compose run --rm binarb python -m binarb order-probe
docker compose run --rm binarb python -m binarb stream-probe
docker compose run --rm binarb python -m binarb scan-once
docker compose --profile operator up -d
```

`order-probe` validates FOK, IOC, and market payloads without execution. The
explicit executing form submits non-crossing FOK/IOC probes and a bounded
BTCUSDT market buy followed immediately by a sell-back. If market-lot rounding
puts the acquired BTC one step below the sell minimum, the probe reconsolidates
that one additional step from the existing BTC balance and reports it:

```bash
docker compose run --rm binarb python -m binarb order-probe --execute \
  --ack I_ACCEPT_MATCHING_ENGINE_PROBE --max-usdt 5.5
```

## Telegram operations

After opening the configured bot and sending `/start`, paste this into
BotFather's `/setcommands` editor:

```text
barb_start - 🟢 Resume Binance triangle scanning and execution
barb_stop - 🟡 Pause new entries; in-flight recovery continues
barb_status - 📊 Show balances, candidates, mode, and last result
barb_clear - 🧹 Archive unresolved local state and remain paused
```

- `/barb_start` changes the desired service state to running. It does not
  override dry-run mode or the separate live-trading acknowledgement.
- `/barb_stop` prevents new opportunities from entering execution. An order
  sequence or recovery already in progress is allowed to finish.
- `/barb_status` reports dry/live mode, desired state, monitored start assets,
  total fee-adjusted USD-equivalent balance (including locked assets), ticker
  and triangle counts, candidate count, and the most recent decision or error.
  Assets without a three-hop USDT valuation are counted explicitly. A delayed
  heartbeat is clearly marked `STALE`.
- `/barb_clear` pauses the strategy and archives all unresolved local deal
  records as operator-cleared. It never places or cancels an exchange order and
  does not rebalance Binance inventory; inspect the account before using it on
  an ambiguous order.

Commands are accepted only from `TG_CHAT_ID`. Run only one `getUpdates`
consumer for a bot token. The operator gateway starts with:

```bash
docker compose --profile operator up -d tg-gateway
```

## Configuration

Copy `.env.example` settings into the secret `.env`. Defaults monitor funded
stablecoin, crypto-hub, and fiat start assets. The configured cap is 100% of
each free start balance; the lazy-arb grid itself never starts above 50%.
Venue minimums and precision filters remain authoritative, so there is no
invalid cross-asset "10 units" assumption.
