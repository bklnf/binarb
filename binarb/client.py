from __future__ import annotations

import hashlib
import hmac
import threading
import time
import urllib.parse
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN

import requests

from .errors import AuthenticationError, BinanceError, RateLimitError
from .models import Book, Edge, Fill, Level, PairMeta


API_ROOT = "https://api.binance.com"


def _d(value, default="0") -> Decimal:
    return Decimal(str(default if value is None else value))


def _plain(value: Decimal) -> str:
    return format(value, "f")


class BinanceClient:
    """Small HMAC REST adapter with serialized signed requests and time sync."""

    def __init__(self, api_key: str, api_secret: str, *, timeout: float = 10,
                 recv_window_ms: int = 5000, session=None, api_root: str = API_ROOT):
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET are required")
        self.api_key = api_key
        self._secret = api_secret.encode()
        self.timeout = timeout
        self.recv_window_ms = recv_window_ms
        self.session = session or requests.Session()
        self.api_root = api_root.rstrip("/")
        self._signed_lock = threading.Lock()
        self._time_offset_ms = 0
        self._account_cache: dict | None = None
        self.pairs: dict[str, PairMeta] = {}
        self.fees: dict[str, Decimal] = {}

    def _raise(self, response, endpoint: str, *, placement: bool = False):
        if response.ok:
            return
        try:
            payload = response.json()
            code, message = payload.get("code"), payload.get("msg", response.text[:200])
        except ValueError:
            code, message = None, f"HTTP {response.status_code}"
        if response.status_code in {418, 429}:
            raise RateLimitError(message, code=code, endpoint=endpoint)
        if code in {-2014, -2015, -1022}:
            raise AuthenticationError(message, code=code, endpoint=endpoint)
        # Binance documents 5xx and -1007 placement outcomes as UNKNOWN.
        ambiguous = placement and (response.status_code >= 500 or code == -1007)
        raise BinanceError(message, code=code, endpoint=endpoint, ambiguous=ambiguous)

    def public(self, endpoint: str, params: dict | None = None):
        response = self.session.get(self.api_root + endpoint, params=params or {}, timeout=self.timeout)
        self._raise(response, endpoint)
        return response.json()

    def sync_time(self) -> int:
        sent = int(time.time() * 1000)
        server = int(self.public("/api/v3/time")["serverTime"])
        received = int(time.time() * 1000)
        self._time_offset_ms = server - (sent + received) // 2
        return self._time_offset_ms

    def signed(self, method: str, endpoint: str, params: dict | None = None,
               *, placement: bool = False):
        with self._signed_lock:
            values = dict(params or {})
            values.update(timestamp=int(time.time() * 1000) + self._time_offset_ms,
                          recvWindow=self.recv_window_ms)
            query = urllib.parse.urlencode(values)
            signature = hmac.new(self._secret, query.encode(), hashlib.sha256).hexdigest()
            signed_query = query + "&signature=" + signature
            request_kwargs = {"params": signed_query} if method.upper() == "GET" else {
                "data": signed_query
            }
            response = self.session.request(
                method, self.api_root + endpoint,
                headers={"X-MBX-APIKEY": self.api_key,
                         "Content-Type": "application/x-www-form-urlencoded"},
                timeout=self.timeout, **request_kwargs,
            )
        self._raise(response, endpoint, placement=placement)
        return response.json()

    @staticmethod
    def _filter(row: dict, name: str) -> dict:
        return next((item for item in row.get("filters", ()) if item.get("filterType") == name), {})

    def load_pair_metadata(self) -> dict[str, PairMeta]:
        payload = self.public("/api/v3/exchangeInfo", {"permissions": "SPOT", "symbolStatus": "TRADING"})
        result = {}
        for row in payload.get("symbols", ()):
            if row.get("status") != "TRADING" or not row.get("isSpotTradingAllowed", False):
                continue
            lot = self._filter(row, "LOT_SIZE")
            market = self._filter(row, "MARKET_LOT_SIZE") or lot
            notional = self._filter(row, "NOTIONAL")
            minimum = self._filter(row, "MIN_NOTIONAL")
            price_filter = self._filter(row, "PRICE_FILTER")
            min_notional = _d(notional.get("minNotional") or minimum.get("minNotional"))
            maximum = _d(notional.get("maxNotional")) if notional.get("maxNotional") else None
            meta = PairMeta(
                symbol=row["symbol"], base=row["baseAsset"], quote=row["quoteAsset"],
                base_step=_d(lot.get("stepSize")), min_qty=_d(lot.get("minQty")),
                max_qty=_d(lot.get("maxQty")), min_notional=min_notional,
                max_notional=maximum, quote_precision=int(row.get("quoteAssetPrecision", 8)),
                market_step=_d(market.get("stepSize")), market_min_qty=_d(market.get("minQty")),
                market_max_qty=_d(market.get("maxQty")),
                price_tick=_d(price_filter.get("tickSize")),
                min_price=_d(price_filter.get("minPrice")),
                max_price=_d(price_filter.get("maxPrice")),
            )
            if meta.base_step > 0 and meta.market_step >= 0:
                result[meta.symbol] = meta
        if not result:
            raise BinanceError("no Binance Spot trading pairs loaded", endpoint="exchangeInfo")
        self.pairs = result
        return result

    def load_account_fees(self) -> dict[str, Decimal]:
        # /sapi/v1/asset/tradeFee is not reachable from every Binance region.
        # The core account response supplies the account's taker tier. Applying
        # it to every pair intentionally ignores zero-fee promotions and BNB
        # discounts, making screening conservative. A shortlisted order is
        # later checked with order/test computeCommissionRates.
        account = self.account()
        rate = _d((account.get("commissionRates") or {}).get("taker"),
                  _d(account.get("takerCommission")) / Decimal(10000))
        if not rate.is_finite() or not Decimal(0) <= rate <= Decimal("0.05"):
            raise BinanceError(f"invalid account taker fee {rate}", endpoint="account")
        self.fees = {symbol: rate for symbol in self.pairs}
        return self.fees

    def tickers(self) -> dict[str, tuple[Decimal, Decimal]]:
        rows = self.public("/api/v3/ticker/bookTicker")
        result = {}
        for row in rows:
            symbol = row.get("symbol")
            if symbol not in self.pairs:
                continue
            bid, ask = _d(row.get("bidPrice")), _d(row.get("askPrice"))
            if bid > 0 and ask >= bid:
                result[symbol] = (bid, ask)
        return result

    def order_book(self, symbol: str, limit: int = 100) -> Book:
        row = self.public("/api/v3/depth", {"symbol": symbol, "limit": limit})
        bids = tuple(Level(_d(p), _d(q)) for p, q in row.get("bids", ()))
        asks = tuple(Level(_d(p), _d(q)) for p, q in row.get("asks", ()))
        if not bids or not asks:
            raise BinanceError(f"one-sided book for {symbol}", endpoint="depth")
        return Book(bids, asks)

    def account(self, *, refresh: bool = False) -> dict:
        if refresh or self._account_cache is None:
            self._account_cache = self.signed(
                "GET", "/api/v3/account", {"omitZeroBalances": "true"}
            )
        return self._account_cache

    def balances(self) -> dict[str, Decimal]:
        row = self.account(refresh=True)
        return {item["asset"]: _d(item.get("free")) for item in row.get("balances", ())}

    def total_balances(self, *, refresh: bool = False) -> dict[str, Decimal]:
        """Free plus locked balances; for reporting only, never order sizing."""
        row = self.account(refresh=refresh)
        return {item["asset"]: _d(item.get("free")) + _d(item.get("locked"))
                for item in row.get("balances", ())}

    def edge(self, source: str, target: str, tickers: dict[str, tuple[Decimal, Decimal]]) -> Edge | None:
        for symbol, meta in self.pairs.items():
            prices, fee = tickers.get(symbol), self.fees.get(symbol)
            if prices is None or fee is None:
                continue
            if source == meta.quote and target == meta.base:
                return Edge(source, target, symbol, "buy", prices[1], fee)
            if source == meta.base and target == meta.quote:
                return Edge(source, target, symbol, "sell", prices[0], fee)
        return None

    @staticmethod
    def _down(value: Decimal, step: Decimal) -> Decimal:
        if not step:
            return value
        return (value / step).to_integral_value(rounding=ROUND_DOWN) * step

    def prepare_market_order(self, edge: Edge, input_amount: Decimal) -> dict:
        meta, amount = self.pairs[edge.symbol], Decimal(input_amount)
        if edge.side == "buy":
            quote_step = Decimal(1).scaleb(-min(meta.quote_precision, 8))
            quote = self._down(amount, quote_step)
            estimated_qty = quote / edge.price
            if quote < meta.min_notional:
                raise ValueError(f"{edge.symbol} quote {quote} below min notional {meta.min_notional}")
            if meta.max_notional is not None and quote > meta.max_notional:
                raise ValueError(f"{edge.symbol} quote {quote} above max notional {meta.max_notional}")
            if meta.market_min_qty and estimated_qty < meta.market_min_qty:
                raise ValueError(f"{edge.symbol} estimated quantity below market minimum")
            if meta.market_max_qty and estimated_qty > meta.market_max_qty:
                raise ValueError(f"{edge.symbol} estimated quantity above market maximum")
            return {"quoteOrderQty": _plain(quote)}
        quantity = self._down(amount, meta.market_step or meta.base_step)
        if quantity < (meta.market_min_qty or meta.min_qty):
            raise ValueError(f"{edge.symbol} quantity {quantity} below market minimum")
        if meta.market_max_qty and quantity > meta.market_max_qty:
            raise ValueError(f"{edge.symbol} quantity {quantity} above market maximum")
        notional = quantity * edge.price
        if notional < meta.min_notional:
            raise ValueError(f"{edge.symbol} notional {notional} below minimum {meta.min_notional}")
        if meta.max_notional is not None and notional > meta.max_notional:
            raise ValueError(f"{edge.symbol} notional {notional} above maximum {meta.max_notional}")
        return {"quantity": _plain(quantity)}

    def prepare_limit_order(self, edge: Edge, input_amount: Decimal,
                            price: Decimal) -> dict:
        """Convert a source-asset budget into Binance LIMIT parameters."""
        meta, amount, raw_price = self.pairs[edge.symbol], Decimal(input_amount), Decimal(price)
        if raw_price <= 0:
            raise ValueError(f"{edge.symbol} limit price must be positive")
        if meta.price_tick:
            rounding = ROUND_CEILING if edge.side == "buy" else ROUND_DOWN
            limit_price = ((raw_price / meta.price_tick).to_integral_value(rounding=rounding)
                           * meta.price_tick)
        else:
            limit_price = raw_price
        if meta.min_price and limit_price < meta.min_price:
            raise ValueError(f"{edge.symbol} price {limit_price} below minimum {meta.min_price}")
        if meta.max_price and limit_price > meta.max_price:
            raise ValueError(f"{edge.symbol} price {limit_price} above maximum {meta.max_price}")
        raw_qty = amount / limit_price if edge.side == "buy" else amount
        quantity = self._down(raw_qty, meta.base_step)
        if quantity < meta.min_qty:
            raise ValueError(f"{edge.symbol} quantity {quantity} below minimum {meta.min_qty}")
        if meta.max_qty and quantity > meta.max_qty:
            raise ValueError(f"{edge.symbol} quantity {quantity} above maximum {meta.max_qty}")
        notional = quantity * limit_price
        if notional < meta.min_notional:
            raise ValueError(f"{edge.symbol} notional {notional} below minimum {meta.min_notional}")
        if meta.max_notional is not None and notional > meta.max_notional:
            raise ValueError(f"{edge.symbol} notional {notional} above maximum {meta.max_notional}")
        return {"quantity": _plain(quantity), "price": _plain(limit_price)}

    def new_limit_order(self, edge: Edge, input_amount: Decimal, price: Decimal,
                        time_in_force: str, client_order_id: str, *, test: bool = False):
        tif = time_in_force.upper()
        if tif not in {"FOK", "IOC"}:
            raise ValueError("triangle limit timeInForce must be FOK or IOC")
        params = {"symbol": edge.symbol, "side": edge.side.upper(), "type": "LIMIT",
                  "timeInForce": tif, "newClientOrderId": client_order_id,
                  "newOrderRespType": "FULL",
                  **self.prepare_limit_order(edge, input_amount, price)}
        endpoint = "/api/v3/order/test" if test else "/api/v3/order"
        row = self.signed("POST", endpoint, params, placement=not test)
        return None if test else self._parse_fill(row)

    def new_market_order(self, edge: Edge, input_amount: Decimal, client_order_id: str,
                         *, test: bool = False):
        params = {"symbol": edge.symbol, "side": edge.side.upper(), "type": "MARKET",
                  "newClientOrderId": client_order_id, "newOrderRespType": "FULL",
                  **self.prepare_market_order(edge, input_amount)}
        endpoint = "/api/v3/order/test" if test else "/api/v3/order"
        row = self.signed("POST", endpoint, params, placement=not test)
        return None if test else self._parse_fill(row)

    def test_commission(self, edge: Edge, input_amount: Decimal) -> Decimal:
        params = {"symbol": edge.symbol, "side": edge.side.upper(), "type": "MARKET",
                  "newClientOrderId": "binarb-fee-probe", "computeCommissionRates": "true",
                  **self.prepare_market_order(edge, input_amount)}
        row = self.signed("POST", "/api/v3/order/test", params)
        total = Decimal(0)
        for name in ("standardCommissionForOrder", "specialCommissionForOrder",
                     "taxCommissionForOrder"):
            total += _d((row.get(name) or {}).get("taker"))
        if not total.is_finite() or not Decimal(0) <= total <= Decimal("0.05"):
            raise BinanceError(f"invalid computed commission {total}", endpoint="order/test")
        return total

    def get_order(self, symbol: str, *, order_id: str | None = None,
                  client_order_id: str | None = None) -> Fill:
        params = {"symbol": symbol}
        if order_id is not None:
            params["orderId"] = order_id
        else:
            params["origClientOrderId"] = client_order_id
        row = self.signed("GET", "/api/v3/order", params)
        if _d(row.get("executedQty")) > 0 and not row.get("fills"):
            trades = self.signed("GET", "/api/v3/myTrades", {
                "symbol": symbol, "orderId": row.get("orderId"), "limit": 1000,
            })
            row["fills"] = trades
        return self._parse_fill(row)

    def cancel_order(self, symbol: str, order_id: str) -> None:
        self.signed("DELETE", "/api/v3/order", {"symbol": symbol, "orderId": order_id})

    @staticmethod
    def _parse_fill(row: dict) -> Fill:
        commissions: dict[str, Decimal] = {}
        for fill in row.get("fills", ()):
            asset = str(fill.get("commissionAsset") or "")
            commissions[asset] = commissions.get(asset, Decimal(0)) + _d(fill.get("commission"))
        return Fill(
            order_id=str(row.get("orderId", "")), client_order_id=str(row.get("clientOrderId", "")),
            status=str(row.get("status", "")).upper(), symbol=str(row.get("symbol", "")),
            side=str(row.get("side", "")).lower(), volume=_d(row.get("executedQty")),
            cost=_d(row.get("cummulativeQuoteQty")), commissions=tuple(commissions.items()),
        )
