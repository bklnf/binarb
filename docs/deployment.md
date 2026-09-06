# Testing, deployment, and rollback

Builds run the full pytest suite before producing the image:

```bash
docker build -t binarb:candidate .
docker compose config --quiet
```

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
