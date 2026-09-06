from decimal import Decimal
from binarb.commissions import CommissionRates
from dataclasses import replace
import json
import time

import pytest

from binarb.executor import Executor, client_id
from binarb.models import Book, Edge, Fill, Level, Opportunity, PairMeta
from binarb.state import StateStore
from binarb.errors import AmbiguousOrderError, BinanceError, RecoveryRequired


EDGES = (Edge("USD", "A", "AUSD", "buy", Decimal("10"), Decimal("0")),
         Edge("A", "B", "AB", "sell", Decimal("2"), Decimal("0")),
         Edge("B", "USD", "BUSD", "sell", Decimal("6"), Decimal("0")))


def meta(symbol, base, quote):
    return PairMeta(symbol, base, quote, Decimal(".0001"), Decimal(".0001"), Decimal("10000"),
                    Decimal("1"), None, 8, Decimal(".0001"), Decimal(".0001"), Decimal("10000"))


class FakeClient:
    def __init__(self):
        self.pairs = {"AUSD": meta("AUSD", "A", "USD"), "AB": meta("AB", "A", "B"),
                      "BUSD": meta("BUSD", "B", "USD")}
        self.balance = {"USD": Decimal("100"), "A": Decimal(0), "B": Decimal(0)}
        self.orders = []
        self.bnb_discount_active = False
    def test_commission(self, edge, amount): return Decimal(0)
    def commission_rates(self, edge, input_amount=None):
        return CommissionRates(self.test_commission(edge, input_amount))
    def order_book(self, symbol):
        book = {"AUSD": Book((Level(9, 100),), (Level(10, 100),)),
                "AB": Book((Level(2, 100),), (Level(2.1, 100),)),
                "BUSD": Book((Level(6, 100),), (Level(6.1, 100),))}[symbol]
        return replace(book, observed_at=time.monotonic())
    def balances(self): return dict(self.balance)
    def prepare_market_order(self, edge, amount): return {"amount": amount}
    def prepare_limit_order(self, edge, amount, price): return {"amount": amount, "price": price}
    def _filled(self, edge, amount, cid, price):
        volume = amount / price if edge.side == "buy" else amount
        cost = amount if edge.side == "buy" else amount * price
        spent = cost if edge.side == "buy" else volume
        self.balance[edge.source] -= spent
        self.balance[edge.target] += volume if edge.side == "buy" else cost
        fill = Fill(str(len(self.orders) + 1), cid, "FILLED", edge.symbol, edge.side, volume, cost)
        self.orders.append(fill); return fill
    def new_limit_order(self, edge, amount, price, tif, cid):
        return self._filled(edge, amount, cid, price)
    def new_market_order(self, edge, amount, cid):
        return self._filled(edge, amount, cid, edge.price)
    def tickers(self): return {"AUSD": (Decimal(9), Decimal(10)), "AB": (Decimal(2), Decimal("2.1")),
                               "BUSD": (Decimal(6), Decimal("6.1"))}
    def edge(self, source, target, tickers): return None


def test_three_filled_legs_complete_and_archive(tmp_path):
    opportunity = Opportunity(("USD", "A", "B", "USD"), EDGES, Decimal("10"),
                              Decimal("12"), Decimal("2000"), Decimal("2"))
    client = FakeClient(); store = StateStore(tmp_path)
    result = Executor(client, store, dry_run=False, poll_interval_s=0,
                      settlement_interval_s=0).execute(opportunity)
    assert result["status"] == "COMPLETE"
    assert result["resized_from"] == Decimal("10")
    assert result["start_amount"] == Decimal("100")
    assert len(client.orders) == 3
    assert not store.active()
    assert len(list(store.archive.glob("*.json"))) == 1


def test_client_order_ids_are_stable_and_bounded():
    assert client_id("deal", "1") == client_id("deal", "1")
    assert client_id("deal", "1") != client_id("deal", "2")
    assert len(client_id("deal", "recovery-long-asset")) <= 36


class EscalatingClient(FakeClient):
    def new_limit_order(self, edge, amount, price, tif, cid):
        if tif == "FOK":
            fill = Fill("1", cid, "EXPIRED", edge.symbol, edge.side, Decimal(0), Decimal(0))
        else:
            spent = amount / 2
            fill = self._filled(edge, spent, cid, price)
            fill = Fill(fill.order_id, fill.client_order_id, "EXPIRED", fill.symbol, fill.side,
                        fill.volume, fill.cost, fill.commissions)
            self.orders[-1] = fill
            return fill
        self.orders.append(fill)
        return fill


def test_leg_escalates_fok_then_ioc_then_market_for_confirmed_remainder(tmp_path):
    client, store = EscalatingClient(), StateStore(tmp_path)
    executor = Executor(client, store, dry_run=False, poll_interval_s=0,
                        settlement_interval_s=0)
    state = {"deal_id": "deal", "status": "INTENT_WRITTEN", "orders": [], "inventory": {}}
    spent, output = executor._run_leg("deal", 1, EDGES[0], Decimal("10"), state)
    assert spent == Decimal("10")
    assert output == Decimal("1")
    assert [order["policy"] for order in state["orders"]] == ["FOK", "IOC", "MARKET"]
    assert [order["input_amount"] for order in state["orders"]] == [
        Decimal("10"), Decimal("10"), Decimal("5")]


def test_full_fok_stops_leg_without_fallback(tmp_path):
    client, store = FakeClient(), StateStore(tmp_path)
    executor = Executor(client, store, dry_run=False)
    state = {"deal_id": "deal", "status": "INTENT_WRITTEN", "orders": [], "inventory": {}}
    spent, output = executor._run_leg("deal", 1, EDGES[0], Decimal("10"), state)
    assert (spent, output) == (Decimal("10"), Decimal("1"))
    assert [order["policy"] for order in state["orders"]] == ["FOK"]


def opportunity():
    return Opportunity(("USD", "A", "B", "USD"), EDGES, Decimal("10"),
                       Decimal("12"), Decimal("2000"), Decimal("2"))


@pytest.mark.parametrize("age,skew,reason", [
    (10, 0, "STALE_QUOTES"), (0, 1, "QUOTE_SKEW"),
])
def test_preflight_rejects_old_or_incoherent_books_without_orders(tmp_path, age, skew, reason):
    client = FakeClient()
    original = client.order_book
    stamp = time.monotonic()
    client.order_book = lambda symbol: replace(
        original(symbol), observed_at=stamp - age - (skew if symbol == "AB" else 0))
    result = Executor(client, StateStore(tmp_path), dry_run=False).execute(opportunity())
    assert result["status"] == "REPRICE_UNPROFITABLE"
    assert result["rejection_reason"] == reason
    assert result["realized_pnl"] == 0
    assert not client.orders


def test_operator_pause_after_confirmation_prevents_entry(tmp_path):
    client = FakeClient()
    result = Executor(client, StateStore(tmp_path), dry_run=False,
                      entry_allowed=lambda: False).execute(opportunity())
    assert result["rejection_reason"] == "OPERATOR_PAUSED"
    assert not client.orders


def test_partial_fill_is_durable_when_later_policy_fails(tmp_path):
    client, store = EscalatingClient(), StateStore(tmp_path)
    def reject(*args):
        raise BinanceError("market rejected")
    client.new_market_order = reject
    state = {"deal_id": "deal", "orders": [], "inventory": {}}
    with pytest.raises(BinanceError):
        Executor(client, store, dry_run=False)._run_leg("deal", 1, EDGES[0], Decimal(10), state)
    saved = json.loads(store.path("deal").read_text())
    assert Decimal(saved["inventory"]["A"]) == Decimal(".5")
    assert Decimal(saved["actual_start_spent"]) == 5


def test_recovery_of_partial_entry_has_cash_pnl_and_fees(tmp_path):
    class Client(EscalatingClient):
        def new_market_order(self, edge, amount, cid):
            if edge.side == "buy":
                raise BinanceError("remainder rejected")
            fill = super().new_market_order(edge, amount, cid)
            return replace(fill, commissions=(("BNB", Decimal(".001")),))
        def edge(self, source, target, tickers):
            if (source, target) == ("A", "USD"):
                return Edge(source, target, "AUSD", "sell", Decimal(9), Decimal(0))
            if (source, target) == ("BNB", "USD"):
                return Edge(source, target, "BNBUSD", "sell", Decimal(100), Decimal(0))
    client, store = Client(), StateStore(tmp_path)
    with pytest.raises(RecoveryRequired):
        Executor(client, store, dry_run=False, settlement_interval_s=0).execute(opportunity())
    saved = json.loads(next(store.archive.glob("*.json")).read_text())
    assert saved["status"] == "RECOVERED"
    assert Decimal(saved["actual_start_spent"]) == 50
    assert Decimal(saved["realized_pnl"]) == Decimal("-5.1")
    assert not store.active()


def test_partial_recovery_does_not_archive_or_retry_same_order(tmp_path):
    class Client(FakeClient):
        def new_limit_order(self, edge, amount, price, tif, cid):
            if edge.symbol == "AB":
                raise BinanceError("restricted symbol")
            return super().new_limit_order(edge, amount, price, tif, cid)
        def edge(self, source, target, tickers):
            return Edge(source, target, "AUSD", "sell", Decimal(9), Decimal(0))
        def new_market_order(self, edge, amount, cid):
            return replace(self._filled(edge, amount / 2, cid, edge.price), status="EXPIRED")
    client, store = Client(), StateStore(tmp_path)
    with pytest.raises(RecoveryRequired):
        Executor(client, store, dry_run=False, settlement_interval_s=0).execute(opportunity())
    active = store.active()[0]
    assert active["status"] == "RECOVERY_REQUIRED"
    assert Decimal(active["inventory"]["A"]) == 5
    assert len(client.orders) == 2
    assert not list(store.archive.glob("*.json"))


def test_ambiguous_recovery_stays_active(tmp_path):
    client, store = FakeClient(), StateStore(tmp_path)
    executor = Executor(client, store, dry_run=False)
    client.edge = lambda *args: Edge("A", "USD", "AUSD", "sell", Decimal(9), Decimal(0))
    def ambiguous(*args, **kwargs):
        raise AmbiguousOrderError("unresolved recovery")
    executor._place_and_resolve = ambiguous
    state = {"deal_id": "deal", "orders": [], "inventory": {"A": Decimal(1)}}
    assert executor._recover_to_start("deal", state, "USD") is False
    assert state["ambiguous_order"] is True


def test_changed_commission_asset_is_persisted_before_recovery(tmp_path):
    client, store = FakeClient(), StateStore(tmp_path)
    original = client.new_limit_order
    client.new_limit_order = lambda *a: replace(original(*a), commissions=(('A', Decimal('.001')),))
    state = {'deal_id': 'fee-change', 'orders': [], 'inventory': {}}
    client.balance['BNB'] = Decimal(1)
    edge = replace(EDGES[0], fee_asset='BNB', fee=Decimal('.001'), fee_conversion=Decimal('.01'))
    with pytest.raises(RecoveryRequired, match='commission asset changed'):
        Executor(client, store, dry_run=False)._run_leg('fee-change', 1, edge, Decimal(10), state)
    saved = json.loads(store.path('fee-change').read_text())
    assert Decimal(saved['inventory']['A']) == Decimal('.999')
    assert Decimal(saved['actual_start_spent']) == 10
    assert Decimal(saved['commissions']['A']) == Decimal('.001')


def test_depleted_external_fee_reserve_prevents_order(tmp_path):
    client, store = FakeClient(), StateStore(tmp_path)
    state = {'deal_id': 'no-reserve', 'orders': [], 'inventory': {}}
    edge = replace(EDGES[0], fee_asset='BNB', fee=Decimal('.001'), fee_conversion=Decimal('.01'))
    with pytest.raises(RecoveryRequired, match='commission reserve depleted'):
        Executor(client, store, dry_run=False)._run_leg('no-reserve', 1, edge, Decimal(10), state)
    assert client.orders == []


def test_external_fee_values_use_persisted_multihop_basis(tmp_path):
    executor = Executor(FakeClient(), StateStore(tmp_path))
    value, unvalued = executor._external_commission_value(
        {'external_commissions': {'BNB': Decimal('.001')},
         'commission_values': {'BNB': Decimal(600)}}, 'USD')
    assert value == Decimal('.6') and not unvalued
