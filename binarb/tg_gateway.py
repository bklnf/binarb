from __future__ import annotations
import logging, time
from datetime import datetime, timezone
from decimal import Decimal
from . import control_plane as cp
from .app import _env, load_env_file, make_client, portfolio_value_usd
from .telegram import TelegramPoller, send_message
from .reporting import TradeReports
ROUTES = {"/barb_start": "start", "/barb_stop": "stop", "/barb_status": "status", "/barb_clear": "clear",
          "/balances": "balances"}

def render_balances():
    client = None
    try:
        client = make_client()
        client.sync_time()
        account = client.account(refresh=True)
        client.load_pair_metadata()
        # Display gross market value, without making fee or order requests.
        client.fees = {symbol: Decimal(0) for symbol in client.pairs}
        tickers = client.tickers()
        observed = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        lines = [f"💰 Binance Spot balances — {observed}",
                 "Showing holdings ≥ $2 (USDT equivalent)."]
        shown, unpriced_count = 0, 0
        def amount(value):
            text = format(value, "f")
            return text.rstrip("0").rstrip(".") if "." in text else text
        for row in sorted(account.get("balances", ()), key=lambda row: row["asset"]):
            free, locked = Decimal(row.get("free", "0")), Decimal(row.get("locked", "0"))
            if free == 0 and locked == 0:
                continue
            value, unpriced = portfolio_value_usd(client, tickers, {row['asset']: free + locked})
            if unpriced:
                unpriced_count += 1
                continue
            if value < Decimal('2'):
                continue
            shown += 1
            lines.append(f"{row['asset']}: total {amount(free + locked)} | "
                         f"available {amount(free)} | locked {amount(locked)} | ≈ ${value:.2f}")
        if not shown:
            lines.append("No priced Spot holdings worth at least $2.")
        if unpriced_count:
            lines.append(f"Unpriced holdings omitted: {unpriced_count}.")
        return "\n".join(lines)
    except Exception as exc:
        # Request exceptions can contain signed URLs; keep details out of replies/logs.
        logging.warning("Telegram balance lookup failed (%s)", type(exc).__name__)
        return "⚠️ Binance balances unavailable. Please try /balances again shortly."
    finally:
        if client is not None:
            client.session.close()

def reply_chunks(text, limit=4000):
    """Keep balance rows together while staying below Telegram's message limit."""
    chunk = ""
    for line in text.splitlines(keepends=True):
        if len(chunk) + len(line) > limit and chunk:
            yield chunk.rstrip("\n")
            chunk = ""
        while len(line) > limit:
            yield line[:limit]
            line = line[limit:]
        chunk += line
    if chunk:
        yield chunk.rstrip("\n")

def render_status():
    row = cp.read_heartbeat()
    if not row: return "⚠️ Binance arb: no heartbeat."
    text = (row.get("texts") or {}).get("status") or "⚠️ Binance arb: status unavailable."
    return f"⚠️ STALE ({int(cp.heartbeat_age(row) or 0)}s old)\n{text}" if cp.is_stale(row) else text
def handle(command):
    parts = command.split(); name = parts[0] if parts else ""; action = ROUTES.get(name)
    if not action: return None
    if len(parts) != 1: return f"Usage: {name}"
    if action == "balances": return render_balances()
    if action == "start": cp.write_control(cp.RUNNING, clear_issued_at=0); return "🟢 Binance arb: resume requested."
    if action == "stop": cp.write_control(cp.PAUSED); return "🟡 Binance arb: pause requested; in-flight recovery continues."
    if action == "clear": cp.request_clear(); return "🧹 Binance arb: local state clear requested; paused with balances unchanged."
    return render_status()
def main():
    load_env_file(); logging.basicConfig(level=_env("ARB_LOG_LEVEL", "INFO")); logging.getLogger("urllib3").setLevel(logging.WARNING)
    if not _env("TG_BOT_TOKEN") or not _env("TG_CHAT_ID"): raise RuntimeError("Telegram settings required")
    poller, started = TelegramPoller(), time.time()
    reports = TradeReports(_env("ARB_STATE_DIR_BINANCE", "data/state"))
    try: send_message("🤖 Binance arb Telegram gateway started.")
    except Exception: logging.exception("Telegram startup notification failed; continuing to poll")
    while True:
        try:
            reports.deliver(send_message)
            for command in poller.poll(timeout=int(_env("TG_GATEWAY_POLL_TIMEOUT", "25")), min_date=started - 10):
                reply = handle(command)
                if reply:
                    for chunk in reply_chunks(reply):
                        send_message(chunk)
        except KeyboardInterrupt: return 0
        except Exception: logging.exception("Telegram poll failed"); time.sleep(1)
if __name__ == "__main__": raise SystemExit(main())
