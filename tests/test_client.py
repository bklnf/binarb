import hashlib
import hmac
from dataclasses import replace
from decimal import Decimal

from binarb.client import BinanceClient
from binarb.models import Edge, PairMeta


class Response:
    def __init__(self, payload, status=200): self.payload, self.status_code = payload, status
    @property
    def ok(self): return self.status_code < 400
    def json(self): return self.payload
    @property
    def text(self): return str(self.payload)


class Session:
    def __init__(self, responses): self.responses, self.calls = list(responses), []
    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs)); return self.responses.pop(0)


def pair():
    return PairMeta("AUSDT", "A", "USDT", Decimal(".01"), Decimal(".01"), Decimal("100"),
                    Decimal("5"), None, 8, Decimal(".01"), Decimal(".01"), Decimal("100"))


def test_signed_get_uses_query_string_and_valid_hmac(monkeypatch):
    session = Session([Response({"ok": True})]); client = BinanceClient("key", "secret", session=session)
    monkeypatch.setattr("binarb.client.time.time", lambda: 1)
    assert client.signed("GET", "/api/v3/account", {"x": "y"}) == {"ok": True}
    kwargs = session.calls[0][2]; query = kwargs["params"]
    unsigned, signature = query.rsplit("&signature=", 1)
    assert "data" not in kwargs
    assert signature == hmac.new(b"secret", unsigned.encode(), hashlib.sha256).hexdigest()


def test_prepare_order_obeys_market_steps_and_quote_budget():
    client = BinanceClient("key", "secret", session=Session([])); client.pairs = {"AUSDT": pair()}
    buy = Edge("USDT", "A", "AUSDT", "buy", Decimal("10"), Decimal(".001"))
    sell = Edge("A", "USDT", "AUSDT", "sell", Decimal("9"), Decimal(".001"))
    assert client.prepare_market_order(buy, Decimal("5.123456789")) == {"quoteOrderQty": "5.12345678"}
    assert client.prepare_market_order(sell, Decimal("1.239")) == {"quantity": "1.23"}


def test_prepare_limit_order_rounds_price_protectively_and_quantity_down():
    client = BinanceClient("key", "secret", session=Session([]))
    client.pairs = {"AUSDT": replace(pair(), price_tick=Decimal(".1"))}
    buy = Edge("USDT", "A", "AUSDT", "buy", Decimal("10"), Decimal(".001"))
    sell = Edge("A", "USDT", "AUSDT", "sell", Decimal("10"), Decimal(".001"))
    assert client.prepare_limit_order(buy, Decimal("10.25"), Decimal("10.01")) == {
        "quantity": "1.01", "price": "10.1"}
    assert client.prepare_limit_order(sell, Decimal("1.239"), Decimal("10.09")) == {
        "quantity": "1.23", "price": "10.0"}


def test_fill_aggregates_commissions_by_asset():
    fill = BinanceClient._parse_fill({"orderId": 1, "clientOrderId": "c", "status": "FILLED",
        "symbol": "AUSDT", "side": "BUY", "executedQty": "2", "cummulativeQuoteQty": "10",
        "fills": [{"commissionAsset": "A", "commission": ".01"},
                  {"commissionAsset": "A", "commission": ".02"},
                  {"commissionAsset": "BNB", "commission": ".001"}]})
    assert fill.commission("A") == Decimal(".03")
    assert fill.commission("BNB") == Decimal(".001")


def test_test_commission_applies_verified_bnb_discount():
    response = Response({"standardCommissionForOrder": {"taker": "0.001"},
                         "specialCommissionForOrder": {"taker": "0"},
                         "taxCommissionForOrder": {"taker": "0"},
                         "discount": {"enabledForAccount": True, "enabledForSymbol": True,
                                      "discountAsset": "BNB", "discount": "0.75"}})
    client = BinanceClient("key", "secret", session=Session([response]))
    client.pairs = {"AUSDT": pair()}
    edge = Edge("USDT", "A", "AUSDT", "buy", Decimal("10"), Decimal(".001"))
    assert client.test_commission(edge, Decimal("5")) == Decimal(".00075")


def test_load_account_fees_prefers_symbol_specific_taker_rates():
    client = BinanceClient("key", "secret", session=Session([
        Response([{"symbol": "AUSDT", "takerCommission": "0.0004"}]),
    ]))
    client.pairs = {"AUSDT": pair(), "BUSDT": replace(pair(), symbol="BUSDT", base="B")}
    client.account = lambda: {"commissionRates": {"taker": "0.001"}}
    assert client.load_account_fees() == {"AUSDT": Decimal(".0004"), "BUSDT": Decimal(".001")}


def test_test_commission_uses_base_rate_when_bnb_discount_is_disabled():
    response = Response({"standardCommissionForOrder": {"taker": "0.001"},
                         "specialCommissionForOrder": {"taker": "0"},
                         "taxCommissionForOrder": {"taker": "0"},
                         "discount": {"enabledForAccount": False, "enabledForSymbol": True,
                                      "discount": "0.75"}})
    client = BinanceClient("key", "secret", session=Session([response]))
    client.pairs = {"AUSDT": pair()}
    edge = Edge("USDT", "A", "AUSDT", "buy", Decimal("10"), Decimal(".001"))
    assert client.test_commission(edge, Decimal("5")) == Decimal(".001")
