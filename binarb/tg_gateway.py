from __future__ import annotations
import logging, time
from . import control_plane as cp
from .app import _env, load_env_file
from .telegram import TelegramPoller, send_message
ROUTES = {"/barb_start": "start", "/barb_stop": "stop", "/barb_status": "status", "/barb_clear": "clear"}
def render_status():
    row = cp.read_heartbeat()
    if not row: return "⚠️ Binance arb: no heartbeat."
    text = (row.get("texts") or {}).get("status") or "⚠️ Binance arb: status unavailable."
    return f"⚠️ STALE ({int(cp.heartbeat_age(row) or 0)}s old)\n{text}" if cp.is_stale(row) else text
def handle(command):
    parts = command.split(); name = parts[0] if parts else ""; action = ROUTES.get(name)
    if not action: return None
    if len(parts) != 1: return f"Usage: {name}"
    if action == "start": cp.write_control(cp.RUNNING, clear_issued_at=0); return "🟢 Binance arb: resume requested."
    if action == "stop": cp.write_control(cp.PAUSED); return "🟡 Binance arb: pause requested; in-flight recovery continues."
    if action == "clear": cp.request_clear(); return "🧹 Binance arb: local state clear requested; paused with balances unchanged."
    return render_status()
def main():
    load_env_file(); logging.basicConfig(level=_env("ARB_LOG_LEVEL", "INFO")); logging.getLogger("urllib3").setLevel(logging.WARNING)
    if not _env("TG_BOT_TOKEN") or not _env("TG_CHAT_ID"): raise RuntimeError("Telegram settings required")
    poller, started = TelegramPoller(), time.time()
    try: send_message("🤖 Binance arb Telegram gateway started.")
    except Exception: logging.exception("Telegram startup notification failed; continuing to poll")
    while True:
        try:
            for command in poller.poll(timeout=int(_env("TG_GATEWAY_POLL_TIMEOUT", "25")), min_date=started - 10):
                reply = handle(command)
                if reply: send_message(reply)
        except KeyboardInterrupt: return 0
        except Exception: logging.exception("Telegram poll failed"); time.sleep(1)
if __name__ == "__main__": raise SystemExit(main())
