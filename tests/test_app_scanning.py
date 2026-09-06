from dataclasses import replace
from decimal import Decimal
from binarb.commissions import CommissionRates
import time

import pytest

from binarb.app import Settings, find_best, run_once
from binarb.errors import RateLimitError
from binarb.market_stream import TickerSnapshot
from binarb.models import Book, Level, PairMeta


class Client:
    def __init__(self):
        self.pairs = {symbol: PairMeta(symbol, base, quote, Decimal(1), Decimal(1),
                                      Decimal(100000), Decimal(1), None, 8,
                                      Decimal(1), Decimal(1), Decimal(100000))
                      for symbol, base, quote in [("AUSDT", "A", "USDT"),
                                                  ("AB", "A", "B"), ("BUSDT", "B", "USDT")]}
        self.fees = {symbol: Decimal(0) for symbol in self.pairs}
        self.prices = {"AUSDT": Decimal(1), "AB": Decimal(1), "BUSDT": Decimal("1.01")}
        self.calls = []
        self.bnb_discount_active = False

    def commission_rates(self, edge, input_amount=None):
        return CommissionRates(self.fees[edge.symbol])

    def tickers(self, age=0):
        return TickerSnapshot({s: (p, p) for s, p in self.prices.items()},
                              observed_at={s: time.monotonic() - age for s in self.pairs})

    def order_book(self, symbol):
        self.calls.append(symbol)
        level = Level(self.prices[symbol], Decimal(10000))
        return Book((level,), (level,), time.monotonic())


def settings():
    return replace(Settings.load(), start_currencies=("USDT",), dry_run=True)


def test_funded_candidate_keeps_cash_and_fetches_each_book_once():
    client = Client()
    candidate, stats = find_best(client, settings(), client.tickers(), {"USDT": Decimal("10.99")})
    assert candidate is not None
    assert candidate.profit == Decimal(".10")
    assert sorted(client.calls) == sorted(client.pairs)
    assert stats["book_candidates"] == stats["confirmed_candidates"] == 1


def test_unfunded_candidate_uses_no_rest_depth():
    client = Client()
    candidate, stats = find_best(client, settings(), client.tickers(), {"USDT": Decimal(".5")})
    assert candidate is None
    assert stats["rejection_codes"] == {"NO_SIZE_GRID": 1}
    assert client.calls == []


def test_stale_and_incoherent_routes_do_not_reach_depth():
    client = Client()
    for snapshot in [client.tickers(age=5), client.tickers()]:
        if max(snapshot.observed_at.values()) > time.monotonic() - 1:
            snapshot.observed_at["AB"] -= 1
        candidate, stats = find_best(client, settings(), snapshot, {"USDT": Decimal(10)})
        assert candidate is None
        assert sum(stats["quote_rejections"].values()) == 2
    assert client.calls == []


def test_stale_depth_fails_closed():
    client = Client()
    original = client.order_book
    client.order_book = lambda s: replace(original(s), observed_at=time.monotonic() - 10)
    candidate, stats = find_best(client, settings(), client.tickers(), {"USDT": Decimal(10)})
    assert candidate is None
    assert stats["rejection_codes"] == {"DEPTH_STALE_QUOTES": 1}


def test_depth_rate_limits_propagate_to_service_backoff():
    client = Client()
    def limited(symbol):
        raise RateLimitError("back off", retry_after_s=5)
    client.order_book = limited
    with pytest.raises(RateLimitError):
        find_best(client, settings(), client.tickers(), {"USDT": Decimal(10)})


def test_cumulative_rejections_survive_a_scan_without_candidates(tmp_path):
    client = Client()
    config = replace(settings(), state_dir=str(tmp_path / "state"))
    runtime = {}
    run_once(client, config, client.tickers(), {"USDT": Decimal(".5")},
             permit_live=False, runtime=runtime)
    run_once(client, config, client.tickers(), {}, permit_live=False, runtime=runtime)
    assert runtime["rejection_codes"] == {}
    assert runtime["total_rejection_codes"] == {"NO_SIZE_GRID": 1}


def test_fee_or_rounding_loss_does_not_trigger_price_disagreement():
    client = Client()
    client.fees = {s: Decimal('.001') for s in client.pairs}
    client.pairs = {s: replace(p, base_step=Decimal('.000001')) for s, p in client.pairs.items()}
    candidate, stats = find_best(client, settings(), client.tickers(), {'USDT': Decimal(100)})
    assert candidate is not None
    assert 'FEED_DISAGREEMENT' not in stats['rejection_codes']


def test_research_can_check_old_tickers_but_cannot_accept_old_depth():
    client = Client()
    snapshot = client.tickers(age=10)
    candidate, _ = find_best(client, settings(), snapshot, {'USDT': Decimal(10)},
                             research_quote_limits=(30, 30))
    assert candidate is not None
    original = client.order_book
    client.order_book = lambda s: replace(original(s), observed_at=time.monotonic() - 10)
    candidate, stats = find_best(client, settings(), snapshot, {'USDT': Decimal(10)},
                                 research_quote_limits=(30, 30))
    assert candidate is None
    assert stats['rejection_codes'] == {'DEPTH_STALE_QUOTES': 1}
