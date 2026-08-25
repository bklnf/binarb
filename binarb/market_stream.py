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
        self.url = url
        self._quotes: dict[str, tuple[Decimal, Decimal, float]] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def seed(self, tickers):
        now = time.monotonic()
        with self._lock:
            self._quotes.update({symbol: (bid, ask, now)
                                 for symbol, (bid, ask) in tickers.items()})

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
            return {symbol: (bid, ask) for symbol, (bid, ask, seen) in self._quotes.items()
                    if seen >= cutoff}

    def ready(self, minimum=1):
        with self._lock:
            return len(self._quotes) >= minimum

    def _run(self, index, symbols):
        import websocket

        backoff = 1.0
        while not self._stop.is_set():
            app = websocket.WebSocketApp(
                self.url,
                on_open=lambda ws: self._subscribe(ws, symbols),
                on_message=lambda _ws, message: self._message(message),
                on_error=lambda _ws, error: logger.warning("market stream %s: %s", index, error),
            )
            app.run_forever(ping_interval=30, ping_timeout=10)
            if self._stop.wait(backoff):
                return
            backoff = min(backoff * 2, 30)

    @staticmethod
    def _subscribe(ws, symbols):
        for index in range(0, len(symbols), 200):
            ws.send(json.dumps({"method": "SUBSCRIBE",
                                "params": [f"{s}@bookTicker" for s in symbols[index:index + 200]],
                                "id": index // 200 + 1}))
            time.sleep(0.3)

    def _message(self, message):
        try:
            row = json.loads(message)
            symbol, bid, ask = row["s"], Decimal(row["b"]), Decimal(row["a"])
            if bid <= 0 or ask < bid:
                return
        except (ValueError, TypeError, KeyError, json.JSONDecodeError):
            return
        with self._lock:
            self._quotes[symbol] = (bid, ask, time.monotonic())
