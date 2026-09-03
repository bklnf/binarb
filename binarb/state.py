from __future__ import annotations

import json
import os
import tempfile
import time
from decimal import Decimal
from pathlib import Path


def _json_default(value):
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"cannot encode {type(value).__name__}")


class StateStore:
    def __init__(self, directory="data/state", *, archive_retention_days=30,
                 archive_max_files=2000):
        self.directory, self.archive = Path(directory), Path(directory) / "archive"
        self.archive_retention_days = int(archive_retention_days)
        self.archive_max_files = int(archive_max_files)

    @property
    def blocked_symbols_path(self):
        # Keep this outside state/ so active() only ever returns deal records.
        return self.directory.parent / "blocked_symbols.json"

    def _read_blocked_symbols(self):
        try:
            data = json.loads(self.blocked_symbols_path.read_text())
        except FileNotFoundError:
            return {}
        except (OSError, ValueError):
            return {}
        return data if isinstance(data, dict) else {}

    def blocked_symbols(self):
        return frozenset(self._read_blocked_symbols())

    def block_symbol(self, symbol, reason):
        """Persist a symbol that Binance rejected for this account."""
        symbol = str(symbol).upper()
        if not symbol:
            return False
        entries = self._read_blocked_symbols()
        if symbol in entries:
            return False
        self.blocked_symbols_path.parent.mkdir(parents=True, exist_ok=True)
        entries[symbol] = {"blocked_at": time.time(), "reason": str(reason)}
        payload = json.dumps(entries, indent=2, sort_keys=True)
        fd, temporary = tempfile.mkstemp(prefix=".blocked_symbols.",
                                         dir=self.blocked_symbols_path.parent)
        try:
            os.fchmod(fd, 0o644)
            with os.fdopen(fd, "w") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.blocked_symbols_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return True

    def seed_blocked_symbols_from_archive(self):
        """Migrate previously observed account-restricted symbols once."""
        if not self.archive.exists():
            return frozenset()
        added = set()
        marker = "symbol is not permitted for this account"
        for path in self.archive.glob("*.json"):
            try:
                state = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            if marker not in str(state.get("error", "")).lower():
                continue
            rejected = next((order for order in reversed(state.get("orders", ()))
                             if order.get("status") == "SUBMITTING" and order.get("symbol")), None)
            if rejected and self.block_symbol(rejected["symbol"], state["error"]):
                added.add(str(rejected["symbol"]).upper())
        return frozenset(added)

    def path(self, deal_id):
        return self.directory / f"{deal_id}.json"

    def save(self, deal_id, state):
        self.directory.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(state, default=_json_default, indent=2, sort_keys=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{deal_id}.", dir=self.directory)
        try:
            os.fchmod(fd, 0o644)
            with os.fdopen(fd, "w") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path(deal_id))
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def active(self):
        if not self.directory.exists():
            return []
        return [json.loads(path.read_text()) for path in sorted(self.directory.glob("*.json"))]

    def finish(self, deal_id, state):
        self.archive.mkdir(parents=True, exist_ok=True)
        self.save(deal_id, state)
        os.replace(self.path(deal_id), self.archive / f"{deal_id}.json")
        self.prune()

    def prune(self, *, now=None):
        if not self.archive.exists():
            return 0
        now = time.time() if now is None else now
        paths = sorted(self.archive.glob("*.json"), key=lambda path: path.stat().st_mtime,
                       reverse=True)
        cutoff, removed = now - self.archive_retention_days * 86400, 0
        for index, path in enumerate(paths):
            if path.stat().st_mtime < cutoff or index >= self.archive_max_files:
                path.unlink()
                removed += 1
        return removed

    def clear_active(self):
        cleared = []
        for state in self.active():
            deal_id = str(state["deal_id"])
            state.update(prior_status=state.get("status"), status="OPERATOR_CLEARED",
                         operator_cleared_at=time.time())
            self.finish(deal_id, state)
            cleared.append(deal_id)
        return cleared
