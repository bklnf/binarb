from decimal import Decimal

from binarb.executor import Executor, client_id
from binarb.models import Book, Edge, Fill, Level, Opportunity, PairMeta
from binarb.state import StateStore


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
    def test_commission(self, edge, amount): return Decimal(0)
    def order_book(self, symbol):
        return {"AUSD": Book((Level(9, 100),), (Level(10, 100),)),
                "AB": Book((Level(2, 100),), (Level(2.1, 100),)),
                "BUSD": Book((Level(6, 100),), (Level(6.1, 100),))}[symbol]
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
