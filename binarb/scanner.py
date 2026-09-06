from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
import math
import time

from .models import Book, Edge, Opportunity, PairMeta
from .commissions import commission_amount, trade_budget


BPS = Decimal(10000)


def build_edges(pairs: dict[str, PairMeta], fees: dict[str, Decimal],
                tickers: dict[str, tuple[Decimal, Decimal]], *,
                blocked_symbols=frozenset(), excluded_assets=frozenset()
                ) -> dict[tuple[str, str], Edge]:
    edges = {}
    for symbol, meta in pairs.items():
        if (symbol in blocked_symbols or meta.base in excluded_assets
                or meta.quote in excluded_assets):
            continue
        prices, fee = tickers.get(symbol), fees.get(symbol)
        if prices is None or fee is None:
            continue
        bid, ask = prices
        if (not all(value.is_finite() for value in (bid, ask, fee))
                or bid <= 0 or ask < bid or not Decimal(0) <= fee <= Decimal("0.05")):
            continue
        observed = getattr(tickers, "observed_at", {}).get(symbol)
        edges[(meta.quote, meta.base)] = Edge(meta.quote, meta.base, symbol, "buy", ask, fee, observed)
        edges[(meta.base, meta.quote)] = Edge(meta.base, meta.quote, symbol, "sell", bid, fee, observed)
    return edges


def discover_triangles(edges: dict[tuple[str, str], Edge], start: str):
    next_by_source: dict[str, set[str]] = {}
    for source, target in edges:
        next_by_source.setdefault(source, set()).add(target)
    routes = set()
    for first in next_by_source.get(start, ()):
        if first == start:
            continue
        for second in next_by_source.get(first, ()):
            if second in {start, first}:
                continue
            if (second, start) in edges:
                routes.add((start, first, second, start))
    return sorted(routes)


def screen_top_of_book(edges, routes, start_amount: Decimal,
                       min_net_bps: Decimal, *, max_age_s=None, max_skew_s=None,
                       diagnostics=None, gross_screen=False) -> tuple[list[Opportunity], Decimal | None]:
    result = []
    best_net_bps = None
    for route in routes:
        route_edges = tuple(edges[(route[index], route[index + 1])] for index in range(3))
        code = observation_error(route_edges, max_age_s=max_age_s, max_skew_s=max_skew_s)
        if code:
            if diagnostics is not None:
                diagnostics[code] = diagnostics.get(code, 0) + 1
            continue
        amount = Decimal(start_amount)
        for edge in route_edges:
            amount = (amount / edge.price if edge.side == "buy" else amount * edge.price)
            amount *= Decimal(1) if gross_screen else Decimal(1) - edge.fee
        net_bps = (amount / start_amount - Decimal(1)) * BPS
        if best_net_bps is None or net_bps > best_net_bps:
            best_net_bps = net_bps
        if net_bps >= min_net_bps:
            result.append(Opportunity(route, route_edges, start_amount, amount,
                                      net_bps, amount - start_amount))
    return sorted(result, key=lambda item: item.net_bps, reverse=True), best_net_bps


def top_of_book_opportunities(edges, routes, start_amount: Decimal,
                              min_net_bps: Decimal) -> list[Opportunity]:
    opportunities, _best_net_bps = screen_top_of_book(
        edges, routes, start_amount, min_net_bps,
    )
    return opportunities


def _down(amount: Decimal, step: Decimal) -> Decimal:
    if not step:
        return amount
    return (amount / step).to_integral_value(rounding=ROUND_DOWN) * step


def observation_error(items, *, max_age_s, max_skew_s, now=None):
    """A timestamp describes request start for REST, receipt for WebSocket."""
    if max_age_s is None and max_skew_s is None:
        return None
    times = [item.observed_at for item in items]
    if not times or any(stamp is None or not math.isfinite(stamp) for stamp in times):
        return "MISSING_OBSERVATION_TIME"
    now = time.monotonic() if now is None else now
    if any(stamp > now for stamp in times):
        return "INVALID_OBSERVATION_TIME"
    if max_age_s is not None and now - min(times) > max_age_s:
        return "STALE_QUOTES"
    if max_skew_s is not None and max(times) - min(times) > max_skew_s:
        return "QUOTE_SKEW"
    return None


def _walk_quantity(levels, quantity):
    remaining, total = quantity, Decimal(0)
    for level in levels:
        take = min(level.quantity, remaining)
        total += take * level.price
        remaining -= take
        if remaining <= 0:
            return total
    return None


@dataclass(frozen=True)
class SimulationResult:
    opportunity: Opportunity | None
    code: str
    leg: int | None = None
    detail: str | None = None


def simulate_detailed(edges: tuple[Edge, Edge, Edge], books: dict[str, Book],
                      pairs: dict[str, PairMeta], start_amount: Decimal) -> SimulationResult:
    amount, route = Decimal(start_amount), [edges[0].source]
    if not amount.is_finite() or amount <= 0:
        return SimulationResult(None, "PAIR_RULES", detail="non-positive start")
    inventory = {edges[0].source: amount}
    external_fees, external_value = {}, Decimal(0)
    for leg, edge in enumerate(edges, 1):
        meta, book = pairs[edge.symbol], books[edge.symbol]
        if (not book.bids or not book.asks
                or any(not Decimal(level.price).is_finite() or not Decimal(level.quantity).is_finite()
                       or level.price <= 0 or level.quantity <= 0
                       for level in (*book.bids, *book.asks))
                or book.bids[0].price > book.asks[0].price):
            return SimulationResult(None, "INVALID_BOOK", leg, edge.symbol)
        trade_amount = trade_budget(edge, amount, book.asks[0].price if edge.side == "buy" else book.bids[0].price)
        if edge.side == "buy":
            budget = _down(trade_amount, Decimal(1).scaleb(-min(meta.quote_precision, 8)))
            remaining, gross = budget, Decimal(0)
            for level in book.asks:
                take = min(level.quantity, remaining / level.price)
                gross += take
                remaining -= take * level.price
                if remaining <= Decimal("0.000000000001"):
                    break
            # quoteOrderQty is converted by the matching engine, but projected
            # output must still be conservative at base LOT_SIZE precision.
            gross = _down(gross, meta.base_step)
            if remaining > Decimal("0.000000000001"):
                return SimulationResult(None, "INSUFFICIENT_DEPTH", leg, edge.symbol)
            consumed = _walk_quantity(book.asks, gross)
            if consumed is None:
                return SimulationResult(None, "INSUFFICIENT_DEPTH", leg, edge.symbol)
            if (gross < meta.min_qty or (meta.max_qty and gross > meta.max_qty)
                    or consumed < meta.min_notional
                    or (meta.max_notional is not None and consumed > meta.max_notional)):
                return SimulationResult(None, "PAIR_RULES", leg, edge.symbol)
        else:
            # The protected FOK/IOC attempts use LOT_SIZE. Market-only rules
            # are checked again if execution reaches the final fallback.
            sell = _down(trade_amount, meta.base_step)
            consumed = sell
            remaining, gross = sell, Decimal(0)
            for level in book.bids:
                take = min(level.quantity, remaining)
                gross += take * level.price
                remaining -= take
                if remaining <= Decimal("0.000000000001"):
                    break
            if remaining > Decimal("0.000000000001"):
                return SimulationResult(None, "INSUFFICIENT_DEPTH", leg, edge.symbol)
            if sell < meta.min_qty or (meta.max_qty and sell > meta.max_qty):
                return SimulationResult(None, "PAIR_RULES", leg, edge.symbol)
            gross = gross.quantize(Decimal(1).scaleb(-min(meta.quote_precision, 8)),
                                   rounding=ROUND_DOWN)
            if (gross < meta.min_notional
                    or (meta.max_notional is not None and gross > meta.max_notional)):
                return SimulationResult(None, "PAIR_RULES", leg, edge.symbol)
        fee_asset = edge.fee_asset or edge.target
        fee_amount = commission_amount(edge, gross)
        if fee_asset == edge.source:
            consumed += fee_amount
        elif fee_asset != edge.target and fee_amount:
            if not edge.fee_value.is_finite() or edge.fee_value <= 0:
                return SimulationResult(None, "COMMISSION_VALUATION_UNAVAILABLE", leg)
            available = edge.fee_available - (start_amount if fee_asset == edges[0].source else 0)
            total = external_fees.get(fee_asset, Decimal(0)) + fee_amount
            if total > available:
                return SimulationResult(None, "COMMISSION_RESERVE", leg)
            external_fees[fee_asset] = total
            external_value += fee_amount * edge.fee_value
        if consumed > amount:
            return SimulationResult(None, "COMMISSION_RESERVE", leg)
        inventory[edge.source] = inventory.get(edge.source, Decimal(0)) - consumed
        amount = gross - (fee_amount if fee_asset == edge.target else 0)
        inventory[edge.target] = inventory.get(edge.target, Decimal(0)) + amount
        route.append(edge.target)
    end = inventory[route[0]]
    unspent_start = end - amount
    residuals = tuple(sorted((asset, value) for asset, value in inventory.items()
                             if asset != route[0] and value > 0))
    end -= external_value
    bps = (end / start_amount - Decimal(1)) * BPS
    return SimulationResult(
        Opportunity(tuple(route), edges, start_amount, end, bps, end - start_amount,
                    unspent_start, residuals, tuple(sorted(external_fees.items())), external_value),
        "OK",
    )


def simulate(edges: tuple[Edge, Edge, Edge], books: dict[str, Book],
             pairs: dict[str, PairMeta], start_amount: Decimal) -> Opportunity | None:
    return simulate_detailed(edges, books, pairs, start_amount).opportunity


def conservative_size_grid(available: Decimal, minimum: Decimal):
    """Sample a dense grid from the configured balance cap to the route floor.

    A four-point halving grid routinely jumped across a narrow executable range.
    The denser geometric grid includes the full configured cap and exact floor
    while allowing quantity/notional filters to find a valid size.
    """
    available, minimum = Decimal(available), Decimal(minimum)
    if minimum <= 0 or available < minimum:
        return []
    maximum = available
    result = [maximum]
    ratio = (minimum / maximum) ** (Decimal(1) / Decimal(15))
    current = maximum
    for _ in range(14):
        current *= ratio
        if current > minimum:
            result.append(current)
    if not result or result[-1] != minimum:
        result.append(minimum)
    return list(dict.fromkeys(result))


def route_minimum_start(edges: tuple[Edge, Edge, Edge],
                        pairs: dict[str, PairMeta]) -> Decimal:
    """Estimate the smallest start amount that can satisfy every leg's filters."""
    factor = Decimal(1)
    required = Decimal(0)
    for edge in edges:
        meta = pairs[edge.symbol]
        if edge.side == "buy":
            local = max(meta.min_notional, meta.min_qty * edge.price)
            conversion = Decimal(1) / edge.price
        else:
            local = max(meta.min_qty, (meta.min_notional / edge.price
                                      if edge.price else Decimal("Infinity")))
            conversion = edge.price
        if edge.fee_asset is None or edge.fee_asset == edge.target:
            conversion *= Decimal(1) - edge.fee
        elif edge.fee_asset == edge.source:
            local *= 1 + edge.fee * edge.fee_conversion * conversion
            conversion /= 1 + edge.fee * edge.fee_conversion * conversion
        if factor > 0:
            required = max(required, local / factor)
        factor *= conversion
    # Quantization and quote rounding can otherwise leave the exact boundary
    # just below a filter. One percent is negligible relative to the live
    # profitability threshold and is revalidated against the actual books.
    return required * Decimal("1.01")


def best_size(edges, books, pairs, minimum, maximum, min_net_bps):
    best, _diagnostic = best_size_detailed(
        edges, books, pairs, minimum, maximum, min_net_bps,
    )
    return best


def best_size_detailed(edges, books, pairs, minimum, maximum, min_net_bps):
    best = None
    diagnostics = {"sizes": 0, "PAIR_RULES": 0, "INSUFFICIENT_DEPTH": 0,
                   "INVALID_BOOK": 0, "BOOK_UNPROFITABLE": 0, "best_net_bps": None,
                   "best_unspent_start": None, "best_residuals": ()}
    for amount in conservative_size_grid(maximum, minimum):
        diagnostics["sizes"] += 1
        result = simulate_detailed(edges, books, pairs, amount)
        candidate = result.opportunity
        if candidate is None:
            diagnostics[result.code] = diagnostics.get(result.code, 0) + 1
            continue
        if (diagnostics["best_net_bps"] is None
                or candidate.net_bps > diagnostics["best_net_bps"]):
            diagnostics["best_net_bps"] = candidate.net_bps
            diagnostics["best_unspent_start"] = candidate.unspent_start
            diagnostics["best_residuals"] = candidate.residuals
        if candidate.net_bps < min_net_bps:
            diagnostics["BOOK_UNPROFITABLE"] += 1
            continue
        if best is None or candidate.profit > best.profit:
            best = candidate
    if best is not None:
        diagnostics["code"] = "ELIGIBLE"
    elif diagnostics["INVALID_BOOK"]:
        diagnostics["code"] = "INVALID_BOOK"
    elif diagnostics["BOOK_UNPROFITABLE"]:
        diagnostics["code"] = "BOOK_UNPROFITABLE"
    elif diagnostics["INSUFFICIENT_DEPTH"]:
        diagnostics["code"] = "INSUFFICIENT_DEPTH"
    elif diagnostics.get("COMMISSION_RESERVE"):
        diagnostics["code"] = "COMMISSION_RESERVE"
    elif diagnostics.get("COMMISSION_VALUATION_UNAVAILABLE"):
        diagnostics["code"] = "COMMISSION_VALUATION_UNAVAILABLE"
    elif diagnostics["PAIR_RULES"]:
        diagnostics["code"] = "PAIR_RULES"
    else:
        diagnostics["code"] = "NO_SIZE_GRID"
    return best, diagnostics
