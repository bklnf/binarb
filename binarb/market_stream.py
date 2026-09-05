from __future__ import annotations

import json
import logging
import threading
import time
from decimal import Decimal

logger = logging.getLogger(__name__)


class BookTickerStream:
    """In-memory latest-value cache; raw market events are never persisted."""

    def __init__(self, symbols, *, url="wss://stream.binance.com:443/ws",
                 max_streams_per_connection=1000):
        symbols = sorted({str(symbol).lower() for symbol in symbols})
        self.groups = [symbols[i:i + max_streams_per_connection]
                       for i in range(0, len(symbols), max_streams_per_connection)]
        self._symbol_group = {symbol.upper(): index for index, group in enumerate(self.groups)
                              for symbol in group}
        self.url = url
        self._quotes: dict[str, tuple[Decimal, Decimal, float, str, int | None]] = {}
        self._health = {index: {"connected": False, "opened_at": None,
                               "last_message_at": None, "reconnects": 0}
                        for index in range(len(self.groups))}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def seed(self, tickers, *, observed_at=None):
        # observed_at should be captured before the REST request. This prevents
        # its response from overwriting a websocket update received in flight.
        seen = time.monotonic() if observed_at is None else float(observed_at)
        with self._lock:
            for symbol, (bid, ask) in tickers.items():
                current = self._quotes.get(symbol)
                if current is None or current[2] <= seen:
                    self._quotes[symbol] = (bid, ask, seen, "REST", None)

    def start(self):
        logger.info("bookTicker stream starting symbols=%d connections=%d persistence=latest-only",
                    sum(len(group) for group in self.groups), len(self.groups))
        for index, group in enumerate(self.groups):
            thread = threading.Thread(target=self._run, args=(index, group),
                                      name=f"book-ticker-{index}", daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self):
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=2)

    def snapshot(self, *, max_age_s=30.0):
        cutoff = time.monotonic() - max_age_s
        with self._lock:
            return {symbol: (row[0], row[1]) for symbol, row in self._quotes.items()
                    for seen in (row[2],)
                    if (seen >= cutoff or self._group_is_fresh(symbol, cutoff))}

    def _group_is_fresh(self, symbol, cutoff):
        group = self._symbol_group.get(symbol)
        if group is None:
            return False
        health = self._health[group]
        return (health["connected"] and health["last_message_at"] is not None
                and health["last_message_at"] >= cutoff)

    def health(self):
        now = time.monotonic()
        with self._lock:
            websocket_quotes = sum(1 for row in self._quotes.values() if row[3] == "WS")
            rest_quotes = len(self._quotes) - websocket_quotes
            connected = sum(1 for row in self._health.values() if row["connected"])
            last_messages = [row["last_message_at"] for row in self._health.values()
                             if row["last_message_at"] is not None]
            return {"connected_groups": connected, "total_groups": len(self.groups),
                    "websocket_quotes": websocket_quotes, "rest_quotes": rest_quotes,
                    "last_message_age_s": (None if not last_messages
                                             else now - max(last_messages)),
                    "reconnects": sum(row["reconnects"] for row in self._health.values())}

    def ready(self, minimum=1):
        with self._lock:
            return len(self._quotes) >= minimum

    def _run(self, index, symbols):
        import websocket

        backoff = 1.0
        while not self._stop.is_set():
            app = websocket.WebSocketApp(
                self.url,
                on_open=lambda ws: self._open(index, ws, symbols),
                on_message=lambda _ws, message: self._message(index, message),
                on_error=lambda _ws, error: logger.warning("market stream %s: %s", index, error),
                on_close=lambda _ws, _code, _reason: self._close(index),
            )
            app.run_forever(ping_interval=30, ping_timeout=10)
            if self._stop.wait(backoff):
                return
            with self._lock:
                opened = self._health[index]["opened_at"]
                lived = time.monotonic() - opened if opened is not None else 0
            backoff = 1.0 if lived >= 30 else min(backoff * 2, 30)

    def _open(self, index, ws, symbols):
        with self._lock:
            row = self._health[index]
            row.update(connected=True, opened_at=time.monotonic())
        self._subscribe(ws, symbols)

    def _close(self, index):
        with self._lock:
            row = self._health[index]
            row["connected"] = False
            row["reconnects"] += 1
            # A reconnect is not a snapshot protocol. Remove every quote from
            # the disconnected subscription so none can be treated as current
            # until a new event or the next REST seed arrives.
            for symbol in self.groups[index]:
                self._quotes.pop(symbol.upper(), None)

    @staticmethod
    def _subscribe(ws, symbols):
        for index in range(0, len(symbols), 200):
            ws.send(json.dumps({"method": "SUBSCRIBE",
                                "params": [f"{s}@bookTicker" for s in symbols[index:index + 200]],
                                "id": index // 200 + 1}))
            time.sleep(0.3)

    def _message(self, index, message=None):
        if message is None:  # direct/unit-test compatibility
            message, index = index, 0
        try:
            row = json.loads(message)
            symbol, bid, ask = row["s"], Decimal(row["b"]), Decimal(row["a"])
            update_id = int(row["u"]) if row.get("u") is not None else None
            if bid <= 0 or ask < bid:
                return
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            return
        with self._lock:
            current = self._quotes.get(symbol)
            if (update_id is not None and current is not None and current[4] is not None
                    and update_id <= current[4]):
                return
            now = time.monotonic()
            self._quotes[symbol] = (bid, ask, now, "WS", update_id)
            self._health[index]["last_message_at"] = now
