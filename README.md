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
- An all-market WebSocket cache retains one bid/ask tuple per symbol in memory,
  rejects out-of-order updates, reports connection health, and defaults to a
  two-second freshness window per symbol. Activity in another symbol never
  refreshes an old quote. Routes also require observation timestamps within
  `ARB_QUOTE_MAX_SKEW_S_BINANCE` (0.5 seconds by default) of each other.
  The ticker shortlist uses gross-price returns as an upper bound, so stale
  estimated commissions cannot suppress a potentially profitable route. Three
  route books and, when needed, one BNB valuation book are fetched concurrently.
  Cached books are reused only while fresh and coherent.
- Side-specific account commissions are cached for 60 seconds, with at most 30
  refresh requests per minute. Deferred refreshes reject the candidate. Each live
  route is revalidated using non-executing `order/test` fee computation and
  fresh depth immediately before leg 1. Only standard commission is discounted;
  tax and special commission remain fully charged.
- Every leg tries fresh top-of-book `LIMIT/FOK`, then `LIMIT/IOC` for any
  confirmed remainder, then `MARKET` for the still-executable remainder. Each
  attempt is fsynced before placement. A transport/5xx timeout
  is resolved using its deterministic `newClientOrderId`; it is never blindly
  retried. Confirmed intermediate inventory is flattened to the start asset on
  ordinary failures. Ambiguous exposure stops for operator recovery.
- Opportunity sizing can use the full configured free-balance cap, derives a
  minimum executable start amount from all three pairs, and
  samples a dense geometric grid down to that exact floor. Immediately before leg 1, fresh balances and all
  three books rerun the complete grid and may resize the opportunity.
  Simulation recomputes the spend for rounded buy quantities and credits cash
  left in the starting asset. Intermediate residuals are reported without
  assuming they can be liquidated. Received-asset fees reduce output; source-asset
  fees reserve part of the order budget. BNB-paid fees preserve acquired tokens
  and are deducted separately from expected profit at a conservative replacement
  value from coherent books, including a 10 bps conversion allowance. Routes
  without a valuation path or sufficient reserve are rejected. BNB-start sizing
  also reserves BNB outside the trading budget for third-asset fees.
  Actual fills remain authoritative. A changed commission asset triggers
  recovery after persisting the confirmed fill. External-fee PnL uses the
  persisted pre-entry valuation basis; intermediate dust remains unvalued.
  The local BNB reserve-maintenance switch does not override exchange-reported
  commission payment eligibility.
- Unfunded routes are rejected before requesting depth. A bounded worker pool
  and fresh book reuse reduce confirmation overhead. Expired or incoherent
  books cannot authorize entry, and an operator pause is checked again before
  the first order. Every confirmed FOK/IOC/MARKET fill updates durable inventory
  before the next attempt; partial or ambiguous recovery leaves the deal active.
- Completed state records are capped at 2,000 files and 30 days by default.
  Docker logs rotate at 30 MB for the strategy and 10 MB for Telegram.
  Candidate books and fee plans are captured under `data/research` at most once
  per 10 seconds, capped at 200 files, 32 MiB, and seven days. Captures are private
  (directory 0700, files 0600) and contain sizing inputs but no credentials or
  account/order responses. Disable with `ARB_CAPTURE_ENABLED_BINANCE=false`.
  Each capture compares both fee models on identical books and size grids.
- Healthy operation emits a `scan heartbeat` every 10 seconds by default,
  including scan count, fresh tickers, triangle/candidate counts, best observed
  signal, decision, and funded start balances. Candidate, depth, execution,
  fill, and recovery decisions are logged as they occur. Configure the cadence
  with `ARB_SCAN_LOG_INTERVAL_S_BINANCE`.
  Heartbeats also include cumulative rejection codes, quote-age/skew exclusions,
  depth request count, scan duration, and the best simulated book return.
  “Depth checked” counts calculations; “Eligible” counts accepted opportunities.
  With strict per-symbol freshness, coverage can fall between REST seeds when
  quiet symbols do not update. The fresh-symbol count now reports that honestly.

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
each free start balance; the sizing grid includes that full configured cap.
Venue minimums and precision filters remain authoritative, so there is no
  invalid cross-asset "10 units" assumption. Restricted bridge assets can be
  removed with `ARB_EXCLUDED_ASSETS_BINANCE` (IDR by default for this account).

Deployment and rollback steps are in [docs/deployment.md](docs/deployment.md).

Offline replay requires no credentials or network:

```bash
python -m binarb.research data/research
```

A bounded observation study can inspect older ticker signals while preserving
the live depth-age/skew checks. It cannot place or cancel orders, never changes
control/deal state, and writes separate captures:

```bash
python -m binarb.research_sample --seconds 60 --output /tmp/binarb-research
python -m binarb.research /tmp/binarb-research/research
```

`strict_ticker_gate` records whether the live quote policy excluded a captured
signal; `price_change_bps` records its disagreement with the reference books.
Replay eligibility means a simulated size cleared fees and filters; it is not
proof of an executable or realized profit. Ticker candidate counters now count
gross-price threshold crossings and are not directly comparable to the prior
net-fee shortlist. See [commission validation](docs/commission-model.md).
