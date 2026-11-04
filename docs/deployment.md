# Testing, deployment, and rollback

Builds run the full pytest suite before producing the image:

```bash
docker build -t binarb:candidate .
docker compose config --quiet
```

The allocation/notification update is prepared for USDC-only triangles,
300-second allocation checks, a 20% relative deviation trigger, and a $10 BNB
reserve with a $3 floor. `rebalance-plan` is a read-only preview:

```bash
docker build -t binarb:allocation-candidate .
docker run --rm --env-file .env -v "$PWD/data:/app/data:ro" \
  binarb:allocation-candidate python -m binarb rebalance-plan
```

Enabling the new live image while RUNNING can immediately submit rebalance orders.
The operator must perform activation. Preserve the image and pause as described
below, inspect the preview and unresolved deals, then replace both strategy and
Telegram gateway so the durable notification outbox is consumed:

```bash
docker image tag binarb:allocation-candidate binarb:latest
docker compose --profile operator up -d --no-build binarb tg-gateway
```

Restore RUNNING using `/barb_start` after checking the paused heartbeat. This
also starts initial allocation maintenance. Do not delete `data/rebalance.json`
to force retries or clear ambiguous active records. Historical PnL import does
not send retrospective Telegram notifications; new completed deals are queued.
Back up `data/trade_reports.sqlite3` along with deal state when preserving history.

Cleanup is checked at every allocation interval, independently of the 20% drift
test. Executable non-USDC holdings are consolidated into USDC. BNB up to the reserve target is retained;
unpriced, restricted and subminimum holdings are reported rather than forced
through an invalid order. `REBALANCE_PARTIAL` can mean the remaining corrections
are below exchange minimums, even after cleanup itself succeeded.

For an isolated ten-minute validation, override dry-run to true, use separate
control/state directories, copy the production blocklist into that separate root,
and disable Telegram credentials. Do not run a second live instance against the
same account. Check the heartbeat immediately after startup and again after at
least 600 seconds: mode, USDC-only starts, increasing scans, both market streams,
maintenance cadence, active deals, errors and restart count. Dry-run maintenance
logs its planned cleanup every configured interval and does not submit orders.

Run a non-executing scan using the existing account configuration and an isolated
state directory. The account data mount is read-only, so this cannot update live
control/deal records. `scan-once` cannot place orders; the dry-run override also
keeps BNB replenishment disabled.

```bash
docker run --rm --env-file .env \
  -e ARB_DRY_RUN_BINANCE=true -e ARB_STATE_DIR_BINANCE=/tmp/review/state \
  -v "$PWD/data:/app/data:ro" binarb:candidate python -m binarb scan-once
```

The IDR asset exclusion is still applied. For other persisted account-restricted
symbols, copy the blocklist into the isolated state's parent directory before
using a shadow scan; never clear the production blocklist to generate candidates.

Before a live deployment, preserve the current image and record the operator
state. Pause new entries via the control plane or `/barb_stop`. Wait for the
heartbeat to acknowledge PAUSED and inspect `data/state/*.json`. Do not restart
with an in-flight or ambiguous deal. Read-only exchange order queries can confirm
that there are no unresolved strategy orders; do not clear state to bypass this.

```bash
docker image tag "$(docker inspect --format '{{.Image}}' binarb)" binarb:rollback
docker exec binarb python -c \
  'from binarb import control_plane as cp; cp.write_control(cp.PAUSED)'
```

Push the tested commit to main before deployment. After confirming the pause and
absence of active deals, replace only the strategy service with the tested image:

```bash
docker image tag binarb:candidate binarb:latest
docker compose up -d --no-build --no-deps binarb
```

Validate the paused startup heartbeat, source/image version, loaded markets,
discount state, and stream connections. Restore RUNNING only if that was the
previous operator state and no recovery is pending:

```bash
docker exec binarb python -c \
  'from binarb import control_plane as cp; cp.write_control(cp.RUNNING)'
```

Existing `ARB_DRY_RUN_BINANCE` and `BINANCE_LIVE_ACK` settings remain authoritative.
Monitor advancing scan counts, fresh quote coverage, cumulative rejection codes,
depth-request failures, and active state. Zero opportunities is a valid outcome;
lowering thresholds is not a deployment acceptance condition. New exceptions,
unresolved orders, or reconciliation failures require pausing new entries.

To roll back, pause and wait for any execution to finish using the same procedure,
then restore the saved image:

```bash
docker image tag binarb:rollback binarb:latest
docker compose up -d --no-build --no-deps binarb
```

New archive fields are additive. Both images preserve active-deal blocking; a
rollback is not permission to discard an unresolved record.
