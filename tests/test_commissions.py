from dataclasses import replace
from decimal import Decimal as D
import json
import os
import time

import pytest

from binarb.client import BinanceClient
from binarb.commissions import (CommissionRates, CommissionUnavailable, commission_amount,
                                parse_rates, plan_edges, reference_symbols)
from binarb.models import Book, Edge, Level, PairMeta
from binarb.research import compare, replay, write_capture
from binarb.scanner import simulate_detailed, screen_top_of_book


def fixture():
    pairs, books = {}, {}
    for base, quote, price, step in [('Z', 'USDT', '1000', '.001'),
                                   ('Z', 'USDC', '1004', '.001'),
                                   ('USDC', 'USDT', '1', '.000001'),
                                   ('BNB', 'USDT', '600', '.00001')]:
        symbol = base + quote
        pairs[symbol] = PairMeta(symbol, base, quote, D(step), D(step), D(100000),
                                 D('.01'), None, 8, D(step), D(step), D(100000))
        level = Level(D(price), D(100000))
        books[symbol] = Book((level,), (level,), time.monotonic())
    edges = (Edge('USDT', 'Z', 'ZUSDT', 'buy', D(1000), D('.00075')),
             Edge('Z', 'USDC', 'ZUSDC', 'sell', D(1004), D('.00075')),
             Edge('USDC', 'USDT', 'USDCUSDT', 'sell', D(1), D('.00075')))
    profiles = (CommissionRates(D('.001'), bnb_eligible=True, multiplier=D('.75')),) * 3
    return edges, profiles, pairs, books


def test_external_bnb_preserves_coarse_lot_and_values_all_fees():
    edges, profiles, pairs, books = fixture()
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D(1)})
    result = simulate_detailed(planned, books, pairs, D(9)).opportunity
    assert result.profit > D('.015')
    assert result.residuals == ()
    assert dict(result.external_fees)['BNB'] > 0
    assert result.end_amount + result.external_fee_value == D('9.036')
    comparison = compare(planned, books, pairs, D(9), D(9), D(5))
    assert comparison['received_asset']['code'] == 'BOOK_UNPROFITABLE'
    assert comparison['asset_aware']['code'] == 'ELIGIBLE'


def test_insufficient_bnb_is_rejected_and_zero_reserve_uses_full_received_fee():
    edges, profiles, pairs, books = fixture()
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D('.000001')})
    assert simulate_detailed(planned, books, pairs, D(9)).code == 'COMMISSION_RESERVE'
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D(0)})
    assert all(e.fee == D('.001') and e.fee_asset == e.target for e in planned)


def test_reserve_exact_boundary_and_missing_value_fail_closed():
    edges, profiles, pairs, books = fixture()
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D(1)})
    reserve = dict(simulate_detailed(planned, books, pairs, D(9)).opportunity.external_fees)['BNB']
    exact = tuple(replace(e, fee_available=reserve) for e in planned)
    assert simulate_detailed(exact, books, pairs, D(9)).code == 'OK'
    depleted = tuple(replace(e, fee_available=reserve - D('.00000001')) for e in planned)
    assert simulate_detailed(depleted, books, pairs, D(9)).code == 'COMMISSION_RESERVE'
    invalid = tuple(replace(e, fee_value=D(0)) for e in planned)
    assert simulate_detailed(invalid, books, pairs, D(9)).code == 'COMMISSION_VALUATION_UNAVAILABLE'


def test_bnb_start_reserves_source_fee_and_external_fees_without_double_counting():
    edges, profiles, pairs, books = fixture()
    edges = (Edge('BNB', 'USDT', 'BNBUSDT', 'sell', D(600), D('.001')),
             edges[0], Edge('Z', 'BNB', 'ZBNB', 'sell', D('1.72'), D('.001')))
    pairs['ZBNB'] = replace(pairs['ZUSDT'], symbol='ZBNB', quote='BNB')
    level = Level(D('1.72'), D(100000))
    books['ZBNB'] = Book((level,), (level,), time.monotonic())
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D(1)})
    result = simulate_detailed(planned, books, pairs, D('.1')).opportunity
    assert result and result.profit > 0
    assert planned[0].fee_asset == planned[0].source
    assert planned[2].fee_asset == planned[2].target
    assert result.external_fee_value > 0
    assert simulate_detailed(planned, books, pairs, D(1)).code == 'COMMISSION_RESERVE'
    client = BinanceClient('test', 'test')
    client.pairs = pairs
    for prepare in (lambda: client.prepare_market_order(planned[0], D('.1')),
                    lambda: client.prepare_limit_order(planned[0], D('.1'), D(600))):
        quantity = D(prepare()['quantity'])
        assert quantity + commission_amount(planned[0], quantity * 600) <= D('.1')


def account_response():
    return {'standardCommission': {'taker': '.001', 'buyer': '.0001', 'seller': '.0002'},
            'taxCommission': {'taker': '.0001', 'buyer': '0', 'seller': '0'},
            'specialCommission': {'taker': '.0002', 'buyer': '0', 'seller': '0'},
            'discount': {'enabledForAccount': True, 'enabledForSymbol': True,
                         'discountAsset': 'BNB', 'discount': '.75'}}


def test_side_rates_and_only_standard_discount_and_zero_promotions():
    row = account_response()
    assert parse_rates(row, 'buy').effective(True) == D('.001125')
    assert parse_rates(row, 'sell').effective(True) == D('.0012')
    order = {k.replace('Commission', 'CommissionForOrder'): v
             for k, v in row.items()}
    assert parse_rates(order, 'buy', for_order=True).effective(True) == D('.00105')
    assert CommissionRates(D(0), D('.0001'), D('.0002'), True, D('.75')).effective(True) == D('.0003')
    row['standardCommission']['taker'] = 'NaN'
    with pytest.raises(CommissionUnavailable):
        parse_rates(row, 'buy')


def test_account_cache_has_ttl_and_bounded_refresh_budget(monkeypatch):
    edges, _, pairs, _ = fixture()
    client = BinanceClient('test', 'test')
    client.pairs = pairs
    calls = []
    client.signed = lambda *a: calls.append(a) or account_response()
    stamp = [100]
    monkeypatch.setattr('binarb.client.time.monotonic', lambda: stamp[0])
    client.commission_rates(edges[0])
    client.commission_rates(replace(edges[0], side='sell'))
    assert len(calls) == 1
    stamp[0] += 61
    client.commission_rates(edges[0])
    assert len(calls) == 2
    client._commission_requests.extend([stamp[0]] * 30)
    with pytest.raises(CommissionUnavailable, match='COMMISSION_REFRESH_BUDGET'):
        client.commission_rates(edges[1])


def test_missing_or_blocked_valuation_bridge_rejects():
    edges, profiles, pairs, _ = fixture()
    assert reference_symbols(edges, profiles, pairs,
                             balances={'BNB': D(1)}) == set(pairs)
    with pytest.raises(CommissionUnavailable):
        reference_symbols(edges, profiles, pairs,
                          balances={'BNB': D(1)}, blocked_symbols={'BNBUSDT'})


def test_gross_screen_retains_signal_hidden_by_stale_fee_estimate():
    edges, _, _, _ = fixture()
    edges = tuple(replace(e, fee=D('.01')) for e in edges)
    graph = {(e.source, e.target): e for e in edges}
    route = [('USDT', 'Z', 'USDC', 'USDT')]
    assert not screen_top_of_book(graph, route, D(9), D(5))[0]
    assert screen_top_of_book(graph, route, D(9), D(5), gross_screen=True)[0]


def test_capture_replay_retention_and_private_allowlist(tmp_path, monkeypatch):
    edges, profiles, pairs, books = fixture()
    planned = plan_edges(edges, profiles, books, pairs, {'BNB': D(1)})
    monkeypatch.setenv('BINANCE_API_SECRET', 'must-never-be-captured')
    monkeypatch.setattr('binarb.research.MAX_FILES', 2)
    for _ in range(3):
        path = write_capture(tmp_path, planned, books, pairs, D(9), D(9), D(5))
    assert len(list(tmp_path.glob('*.json'))) == 2
    assert path.stat().st_mode & 0o777 == 0o600
    assert tmp_path.stat().st_mode & 0o777 == 0o700
    assert 'must-never-be-captured' not in path.read_text()
    data = json.loads(path.read_text())
    assert json.loads(json.dumps(replay(path), default=str)) == data['comparison']
    old = list(tmp_path.glob('*.json'))[0]
    os.utime(old, (1, 1))
    write_capture(tmp_path, planned, books, pairs, D(9), D(9), D(5))
    assert not old.exists()


def test_research_guard_prohibits_real_orders_and_cancellations():
    from binarb.research_sample import restrict_to_observation
    client = BinanceClient('test', 'test')
    calls = []
    client.signed = lambda *a, **kw: calls.append(a)
    restrict_to_observation(client)
    client.signed('GET', '/api/v3/account')
    client.signed('POST', '/api/v3/order/test')
    for method in ('POST', 'DELETE'):
        with pytest.raises(RuntimeError, match='prohibits exchange mutations'):
            client.signed(method, '/api/v3/order')
    assert len(calls) == 2


def test_three_leg_execution_keeps_whole_lots_and_accounts_external_fees(tmp_path):
    from binarb.executor import Executor
    from binarb.models import Fill
    from binarb.state import StateStore
    edges, profiles, pairs, books = fixture()
    client = BinanceClient('test', 'test')
    client.pairs = pairs
    balance = {'USDT': D(9), 'Z': D(0), 'USDC': D(0), 'BNB': D(1)}
    client.balances = lambda: dict(balance)
    client.commission_rates = lambda *a: profiles[0]
    client.order_book = lambda s: replace(books[s], observed_at=time.monotonic())
    client.edge = lambda *a: None
    fills = []

    def fill(edge, amount, price, tif, cid):
        params = client.prepare_limit_order(edge, amount, price)
        quantity, price = D(params['quantity']), D(params['price'])
        cost = quantity * price
        output = quantity if edge.side == 'buy' else cost
        spent = cost if edge.side == 'buy' else quantity
        fee = commission_amount(edge, output)
        balance[edge.source] -= spent
        balance[edge.target] += output
        balance[edge.fee_asset] -= fee
        result = Fill(str(len(fills) + 1), cid, 'FILLED', edge.symbol, edge.side,
                      quantity, cost, ((edge.fee_asset, fee),))
        fills.append(result)
        return result

    client.new_limit_order = fill
    planned = plan_edges(edges, profiles, books, pairs, balance)
    opportunity = simulate_detailed(planned, books, pairs, D(9)).opportunity
    state = Executor(client, StateStore(tmp_path), dry_run=False, min_net_bps=D(5),
                     settlement_interval_s=0).execute(opportunity)
    assert len(fills) == 3
    assert fills[0].volume == fills[1].volume == D('.009')
    assert state['status'] == 'COMPLETE'
    assert state['realized_pnl'] == opportunity.profit > 0
    assert balance['BNB'] < 1
