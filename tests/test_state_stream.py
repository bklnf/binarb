import os
from decimal import Decimal

from binarb.market_stream import BookTickerStream
from binarb.state import StateStore


def test_stream_keeps_only_latest_values_in_memory():
    stream = BookTickerStream(["AUSDT"])
    stream._message('{"s":"AUSDT","b":"1.2","a":"1.3"}')
    stream._message('{"s":"AUSDT","b":"1.4","a":"1.5"}')
    assert stream.snapshot()["AUSDT"] == (Decimal("1.4"), Decimal("1.5"))
    assert len(stream._quotes) == 1


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
