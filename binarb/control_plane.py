from __future__ import annotations
import json, os, tempfile, time
from decimal import Decimal
from pathlib import Path

RUNNING, PAUSED = "running", "paused"
def _root(): return Path(os.environ.get("ARB_CONTROL_ROOT_BINANCE", "data"))
def control_path(): return _root() / "control" / "arb_binance.json"
def status_path(): return _root() / "status" / "arb_binance.json"
def _json_default(value):
    if isinstance(value, Decimal): return str(value)
    raise TypeError(type(value).__name__)
def _write(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o644)
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, default=_json_default)
            stream.flush(); os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)
def _read(path):
    try: return json.loads(path.read_text())
    except (OSError, ValueError): return None
def read_control():
    row = _read(control_path()) or {}; desired = row.get("desired_state", RUNNING)
    return {"desired_state": desired if desired in {RUNNING, PAUSED} else PAUSED,
            "clear_issued_at": float(row.get("clear_issued_at") or 0)}
def write_control(desired_state, *, clear_issued_at=None):
    if desired_state not in {RUNNING, PAUSED}: raise ValueError(desired_state)
    old = read_control(); _write(control_path(), {"desired_state": desired_state,
        "clear_issued_at": old["clear_issued_at"] if clear_issued_at is None else clear_issued_at,
        "updated_at": time.time()})
def request_clear(): write_control(PAUSED, clear_issued_at=time.time())
def consume_clear(): write_control(PAUSED, clear_issued_at=0)
def write_heartbeat(state, text, runtime, *, interval=2, started_at=None):
    _write(status_path(), {"state": state, "updated_at": time.time(), "interval": interval,
                           "started_at": started_at, "texts": {"status": text}, "runtime": runtime})
def read_heartbeat(): return _read(status_path())
def heartbeat_age(row): return None if not row else max(0, time.time() - float(row.get("updated_at") or 0))
def is_stale(row):
    age = heartbeat_age(row); return age is None or age > 3 * float(row.get("interval") or 2)
