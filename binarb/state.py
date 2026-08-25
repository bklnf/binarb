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
