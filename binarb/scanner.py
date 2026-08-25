from __future__ import annotations

from decimal import Decimal, ROUND_DOWN

from .models import Book, Edge, Opportunity, PairMeta


BPS = Decimal(10000)


def build_edges(pairs: dict[str, PairMeta], fees: dict[str, Decimal],
                tickers: dict[str, tuple[Decimal, Decimal]]) -> dict[tuple[str, str], Edge]:
    edges = {}
    for symbol, meta in pairs.items():
        prices, fee = tickers.get(symbol), fees.get(symbol)
        if prices is None or fee is None:
            continue
        bid, ask = prices
        if bid <= 0 or ask < bid or not Decimal(0) <= fee <= Decimal("0.05"):
            continue
        edges[(meta.quote, meta.base)] = Edge(meta.quote, meta.base, symbol, "buy", ask, fee)
        edges[(meta.base, meta.quote)] = Edge(meta.base, meta.quote, symbol, "sell", bid, fee)
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
                       min_net_bps: Decimal) -> tuple[list[Opportunity], Decimal | None]:
    result = []
    best_net_bps = None
    for route in routes:
        route_edges = tuple(edges[(route[index], route[index + 1])] for index in range(3))
        amount = Decimal(start_amount)
        for edge in route_edges:
            amount = (amount / edge.price if edge.side == "buy" else amount * edge.price)
            amount *= Decimal(1) - edge.fee
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


def simulate(edges: tuple[Edge, Edge, Edge], books: dict[str, Book],
             pairs: dict[str, PairMeta], start_amount: Decimal) -> Opportunity | None:
    amount, route = Decimal(start_amount), [edges[0].source]
    for edge in edges:
        meta, book = pairs[edge.symbol], books[edge.symbol]
        if edge.side == "buy":
            budget, remaining, gross = amount, amount, Decimal(0)
            for level in book.asks:
                take = min(level.quantity, remaining / level.price)
                gross += take
                remaining -= take * level.price
                if remaining <= Decimal("0.000000000001"):
                    break
            # quoteOrderQty is converted by the matching engine, but projected
            # output must still be conservative at base LOT_SIZE precision.
            gross = _down(gross, meta.base_step)
            consumed = budget - max(remaining, Decimal(0))
            if (remaining > Decimal("0.000000000001") or gross < meta.min_qty
                    or (meta.max_qty and gross > meta.max_qty)
                    or consumed < meta.min_notional
                    or (meta.max_notional is not None and consumed > meta.max_notional)):
                return None
        else:
            # The protected FOK/IOC attempts use LOT_SIZE. Market-only rules
            # are checked again if execution reaches the final fallback.
            sell = _down(amount, meta.base_step)
            remaining, gross = sell, Decimal(0)
            for level in book.bids:
                take = min(level.quantity, remaining)
                gross += take * level.price
                remaining -= take
                if remaining <= Decimal("0.000000000001"):
                    break
            if (remaining > Decimal("0.000000000001") or sell < meta.min_qty
                    or (meta.max_qty and sell > meta.max_qty)):
                return None
            gross = gross.quantize(Decimal(1).scaleb(-min(meta.quote_precision, 8)),
                                   rounding=ROUND_DOWN)
            if gross < meta.min_notional or (meta.max_notional is not None and gross > meta.max_notional):
                return None
        amount = gross * (Decimal(1) - edge.fee)
        route.append(edge.target)
    bps = (amount / start_amount - Decimal(1)) * BPS
    return Opportunity(tuple(route), edges, start_amount, amount, bps, amount - start_amount)


def conservative_size_grid(available: Decimal, minimum: Decimal):
    """Lazy-arb sizing: half available first, then halve to the exact floor."""
    available, minimum = Decimal(available), Decimal(minimum)
    if minimum <= 0 or available < minimum:
        return []
    result = []
    current = available / Decimal(2)
    while current >= minimum:
        result.append(current)
        current /= Decimal(2)
    if not result or result[-1] != minimum:
        result.append(minimum)
    return list(dict.fromkeys(result))


def best_size(edges, books, pairs, minimum, maximum, min_net_bps):
    best = None
    for amount in conservative_size_grid(maximum, minimum):
        candidate = simulate(edges, books, pairs, amount)
        if candidate is None or candidate.net_bps < min_net_bps:
            continue
        if best is None or candidate.profit > best.profit:
            best = candidate
    return best
