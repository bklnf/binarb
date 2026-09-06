from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class PairMeta:
    symbol: str
    base: str
    quote: str
    base_step: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    max_notional: Decimal | None
    quote_precision: int
    market_step: Decimal
    market_min_qty: Decimal
    market_max_qty: Decimal
    price_tick: Decimal = Decimal(0)
    min_price: Decimal = Decimal(0)
    max_price: Decimal = Decimal(0)
    min_notional_apply_market: bool = True
    max_notional_apply_market: bool = True
    quote_order_qty_market_allowed: bool = True


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    symbol: str
    side: str
    price: Decimal
    fee: Decimal
    observed_at: float | None = None


@dataclass(frozen=True)
class Level:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class Book:
    bids: tuple[Level, ...]
    asks: tuple[Level, ...]
    observed_at: float | None = None


@dataclass(frozen=True)
class Opportunity:
    route: tuple[str, str, str, str]
    edges: tuple[Edge, Edge, Edge]
    start_amount: Decimal
    end_amount: Decimal
    net_bps: Decimal
    profit: Decimal
    # Cash left in the starting asset is fully valued. Intermediate dust is
    # reported separately and receives no speculative liquidation credit.
    unspent_start: Decimal = Decimal(0)
    residuals: tuple[tuple[str, Decimal], ...] = ()


@dataclass(frozen=True)
class Fill:
    order_id: str
    client_order_id: str
    status: str
    symbol: str
    side: str
    volume: Decimal
    cost: Decimal
    commissions: tuple[tuple[str, Decimal], ...] = ()

    @property
    def terminal(self) -> bool:
        return self.status in {"FILLED", "CANCELED", "REJECTED", "EXPIRED", "EXPIRED_IN_MATCH"}

    def commission(self, asset: str) -> Decimal:
        return sum((amount for name, amount in self.commissions if name == asset), Decimal(0))
