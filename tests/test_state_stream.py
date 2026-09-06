import os
import time
from decimal import Decimal

from binarb.market_stream import BookTickerStream
from binarb.state import StateStore


def test_stream_keeps_only_latest_values_in_memory():
    stream = BookTickerStream(["AUSDT"])
    stream._message('{"s":"AUSDT","b":"1.2","a":"1.3"}')
    stream._message('{"s":"AUSDT","b":"1.4","a":"1.5"}')
    assert stream.snapshot()["AUSDT"] == (Decimal("1.4"), Decimal("1.5"))
    assert len(stream._quotes) == 1


def test_stream_ignores_out_of_order_updates_and_rest_seed_does_not_clobber_ws():
    stream = BookTickerStream(["AUSDT"])
    stream._message('{"s":"AUSDT","b":"1.4","a":"1.5","u":2}')
    stream._message('{"s":"AUSDT","b":"1.2","a":"1.3","u":1}')
    stream.seed({"AUSDT": (Decimal("1.0"), Decimal("1.1"))}, observed_at=0)
    assert stream.snapshot()["AUSDT"] == (Decimal("1.4"), Decimal("1.5"))
    assert stream.health()["websocket_quotes"] == 1


def test_disconnect_invalidates_all_quotes_owned_by_that_connection():
    stream = BookTickerStream(["AUSDT", "BUSDT"])
    stream.seed({"AUSDT": (Decimal("1"), Decimal("1.1")),
                 "BUSDT": (Decimal("2"), Decimal("2.1"))})
    stream._health[0].update(connected=True, last_message_at=10)
    stream._close(0)
    assert stream.snapshot() == {}


def test_archive_retention_is_bounded(tmp_path):
    store = StateStore(tmp_path, archive_retention_days=100000, archive_max_files=2)
    for index in range(3):
        store.save(str(index), {"deal_id": str(index), "value": Decimal("1")})
        store.finish(str(index), {"deal_id": str(index)})
    assert len(list(store.archive.glob("*.json"))) == 2

    oldest = min(store.archive.glob("*.json"), key=lambda path: path.stat().st_mtime)
    os.utime(oldest, (100, 100))
    store.archive_retention_days = 1
    assert store.prune(now=100 + 86401) == 1


def test_unrelated_symbol_activity_cannot_refresh_old_quote():
    stream = BookTickerStream(["AUSDT", "BUSDT"])
    stream.seed({"AUSDT": (Decimal(1), Decimal(1))}, observed_at=time.monotonic() - 60)
    stream._health[0]["connected"] = True
    stream._message('{"s":"BUSDT","b":"1","a":"1","u":1}')
    snapshot = stream.snapshot(max_age_s=2)
    assert "AUSDT" not in snapshot
    assert "BUSDT" in snapshot
    assert snapshot.observed_at.keys() == snapshot.keys()
    assert stream.health()["stale_quotes"] == 1


def test_rest_seed_preserves_websocket_update_watermark():
    stream = BookTickerStream(["AUSDT"])
    stream._message('{"s":"AUSDT","b":"2","a":"2","u":20}')
    stream.seed({"AUSDT": (Decimal(3), Decimal(3))})
    stream._message('{"s":"AUSDT","b":"1","a":"1","u":19}')
    assert stream.snapshot()["AUSDT"] == (Decimal(3), Decimal(3))


def test_unknown_symbols_and_non_finite_quotes_are_ignored():
    stream = BookTickerStream(["AUSDT"])
    for message in ['{"s":"XUSDT","b":"1","a":"1"}',
                    '{"s":"AUSDT","b":"NaN","a":"1"}',
                    '{"s":"AUSDT","b":"1","a":"Infinity"}']:
        stream._message(message)
    assert stream.snapshot() == {}
