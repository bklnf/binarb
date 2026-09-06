"""Asset-aware commission plans, independently implemented from API semantics."""
from collections import deque
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_CEILING


D = Decimal
QUANTUM = D('0.00000001')
CONVERSION_BUFFER = D('1.001')


class CommissionUnavailable(ValueError):
    pass


@dataclass(frozen=True)
class CommissionRates:
    standard: Decimal
    tax: Decimal = D(0)
    special: Decimal = D(0)
    bnb_eligible: bool = False
    multiplier: Decimal = D(1)

    def effective(self, bnb):
        return self.standard * (self.multiplier if bnb and self.bnb_eligible else 1) + self.tax + self.special


def parse_rates(row, side, *, for_order=False):
    suffix = 'CommissionForOrder' if for_order else 'Commission'
    rates = []
    for kind in ('standard', 'tax', 'special'):
        values = row[kind + suffix]
        rate = D(str(values['taker']))
        if not for_order:
            rate += D(str(values['buyer' if side == 'buy' else 'seller']))
        if not rate.is_finite() or not 0 <= rate <= D('.05'):
            raise CommissionUnavailable('INVALID_COMMISSION_RATE')
        rates.append(rate)
    discount = row['discount']
    multiplier = D(str(discount['discount']))
    if not multiplier.is_finite() or not 0 <= multiplier <= 1 or sum(rates) > D('.05'):
        raise CommissionUnavailable('INVALID_COMMISSION_RATE')
    eligible = (discount.get('enabledForAccount') is True
                and discount.get('enabledForSymbol') is True
                and discount.get('discountAsset') == 'BNB')
    return CommissionRates(*rates, eligible, multiplier)


def reference_symbols(edges, profiles, pairs, *, balances,
                      blocked_symbols=frozenset(), excluded_assets=frozenset()):
    symbols = {e.symbol for e in edges}
    if balances.get('BNB', D(0)) <= 0 or not any(
            p.bnb_eligible and p.effective(True) > 0 for p in profiles):
        return symbols
    assets = {e.source for e in edges}
    if 'BNB' in assets:
        return symbols
    # One bridge is sufficient because the route itself connects all assets.
    allowed = {s: p for s, p in pairs.items() if s not in blocked_symbols
               and not {p.base, p.quote} & set(excluded_assets)}
    for asset in dict.fromkeys([edges[0].source, 'USDT', 'BTC', *sorted(assets)]):
        if asset not in assets:
            continue
        for symbol, p in sorted(allowed.items()):
            if {p.base, p.quote} == {'BNB', asset}:
                return symbols | {symbol}
    raise CommissionUnavailable('COMMISSION_VALUATION_UNAVAILABLE')


def conversion(source, target, books, pairs):
    """Upper replacement value along a deterministic shortest path."""
    graph = {}
    for symbol, book in sorted(books.items()):
        p = pairs[symbol]
        if not book.bids or not book.asks:
            raise CommissionUnavailable('COMMISSION_INVALID_BOOK')
        bid, ask = D(book.bids[0].price), D(book.asks[0].price)
        if not bid.is_finite() or not ask.is_finite() or bid <= 0 or ask < bid:
            raise CommissionUnavailable('COMMISSION_INVALID_BOOK')
        graph.setdefault(p.base, []).append((p.quote, ask))
        graph.setdefault(p.quote, []).append((p.base, 1 / bid))
    queue, seen = deque([(source, D(1))]), {source}
    while queue:
        asset, value = queue.popleft()
        if asset == target:
            return value
        for other, rate in graph.get(asset, ()):
            if other not in seen:
                seen.add(other)
                queue.append((other, value * rate))
    raise CommissionUnavailable('COMMISSION_VALUATION_UNAVAILABLE')


def plan_edges(edges, profiles, books, pairs, balances):
    result = []
    for edge, profile in zip(edges, profiles, strict=True):
        bnb = profile.bnb_eligible and balances.get('BNB', D(0)) > 0
        asset = 'BNB' if bnb and profile.effective(True) else edge.target
        external = asset not in {edge.source, edge.target}
        factor = conversion(edge.target, asset, books, pairs) if asset != edge.target else D(1)
        # Includes a small conversion/rounding allowance; no fee discount is
        # applied to taxes or special commission rates.
        if asset != edge.target:
            factor *= CONVERSION_BUFFER
        price = books[edge.symbol].asks[0].price if edge.side == 'buy' else books[edge.symbol].bids[0].price
        result.append(replace(edge, price=D(price), fee=profile.effective(bnb), fee_asset=asset,
                              fee_conversion=factor,
                              fee_value=(conversion(asset, edges[0].source, books, pairs)
                                         * CONVERSION_BUFFER if external else D(0)),
                              fee_available=balances.get(asset, D(0)),
                              observed_at=books[edge.symbol].observed_at))
    return tuple(result)


def commission_amount(edge, gross):
    value = gross * edge.fee * edge.fee_conversion
    return value.quantize(QUANTUM, rounding=ROUND_CEILING) if edge.fee_asset else value


def trade_budget(edge, amount, price):
    if edge.fee_asset != edge.source or not edge.fee:
        return amount
    factor = (1 / D(price) if edge.side == 'buy' else D(price)) * edge.fee_conversion
    return max(D(0), (amount - QUANTUM) / (1 + edge.fee * factor))


def price_change_bps(edges, books):
    old, new = D(1), D(1)
    for edge in edges:
        price = D(books[edge.symbol].asks[0].price if edge.side == 'buy' else books[edge.symbol].bids[0].price)
        old *= 1 / edge.price if edge.side == 'buy' else edge.price
        new *= 1 / price if edge.side == 'buy' else price
    return abs(new / old - 1) * 10000
