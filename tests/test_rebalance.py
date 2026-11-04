from dataclasses import replace
from decimal import Decimal as D
import time

import pytest

from binarb.app import Settings
from binarb.client import BinanceClient
from binarb.errors import AmbiguousOrderError, BinanceError, RecoveryRequired
from binarb.commissions import CommissionRates
from binarb.market_stream import TickerSnapshot
from binarb.models import Book, Fill, Level, PairMeta
from binarb.rebalance import Rebalancer, allocation, conversion_path


class Client:
    prepare_limit_order = BinanceClient.prepare_limit_order
    _down = staticmethod(BinanceClient._down)
    def __init__(self):
        self.prices = {'USDCUSDT': D(1), 'ETHUSDT': D(2000), 'BTCUSDT': D(50000), 'BNBUSDT': D(500),
                       'OLDUSDT': D(2)}
        self.pairs = {s: PairMeta(s, s[:-4], 'USDT', D('.000001'), D('.000001'),
                                 D(1000000), D(5), None, 8, D('.000001'), D('.000001'), D(1000000))
                      for s in self.prices}
        self.fees = {s: D(0) for s in self.prices}
        self.balance = {'USDT': D(10), 'USDC': D(0), 'ETH': D('.005'), 'BTC': D('.0014'),
                        'BNB': D('.02'), 'OLD': D(5)}
        self.orders = []
    def balances(self): return dict(self.balance)
    def total_balances(self): return dict(self.balance)
    def tickers(self):
        return TickerSnapshot({s: (p, p) for s, p in self.prices.items()},
                              observed_at={s: time.monotonic() for s in self.prices})
    def commission_rates(self, edge, amount=None): return CommissionRates(D(0))
    def order_book(self, symbol):
        level = Level(self.prices[symbol], D(100000))
        return Book((level,), (level,), time.monotonic())
    def new_limit_order(self, edge, amount, price, tif, cid):
        params = self.prepare_limit_order(edge, amount, price)
        qty, cost = D(params['quantity']), D(params['quantity']) * D(params['price'])
        self.balance[edge.source] -= cost if edge.side == 'buy' else qty
        self.balance[edge.target] = self.balance.get(edge.target, D(0)) + (qty if edge.side == 'buy' else cost)
        fill = Fill(str(len(self.orders)), cid, 'FILLED', edge.symbol, edge.side, qty, cost)
        self.orders.append(fill)
        return fill


def setup(tmp_path, **kwargs):
    config = replace(Settings.load(), state_dir=str(tmp_path / 'state'), dry_run=False,
                     start_currencies=('USDC',), rebalance_enabled=True,
                     use_bnb_fee_discount=True, bnb_replenish_enabled=True, **kwargs)
    client = Client()
    return client, Rebalancer(client, config, entry_allowed=lambda: True)


def test_initial_conversion_consolidates_usdc_and_preserves_bnb(tmp_path):
    client, manager = setup(tmp_path)
    result = manager.run()
    assert result['status'] == 'REBALANCE_COMPLETE'
    assert result['after']['target_usdt'] == 100
    assert abs(client.balance['USDC'] - 100) < D('.01')
    assert all(client.balance[a] == 0 for a in ('USDT', 'ETH', 'BTC'))
    assert client.balance['BNB'] == D('.02')
    assert not manager.store.active()
    assert not manager.due()
    assert manager.due(time.time() + 301)
    assert manager.run()['status'] == 'REBALANCE_BALANCED'


def test_threshold_is_strictly_twenty_percent_of_target(tmp_path):
    client, manager = setup(tmp_path)
    client.balance = {'USDT': D(20), 'USDC': D(80), 'BNB': D('.02')}
    assert manager.snapshot()['triggered'] is False
    client.balance['USDT'] += D('.01')
    client.balance['USDC'] -= D('.01')
    assert manager.snapshot()['triggered'] is True


def test_dry_run_pause_and_active_deals_never_order(tmp_path):
    client, manager = setup(tmp_path)
    manager.settings = replace(manager.settings, dry_run=True)
    assert manager.run()['status'] == 'REBALANCE_DRY_RUN'
    assert not manager.schedule.exists()
    manager.settings = replace(manager.settings, dry_run=False)
    manager.entry_allowed = lambda: False
    assert manager.run()['status'] == 'REBALANCE_PAUSED'
    manager.store.save('unfinished', {'deal_id': 'unfinished'})
    with pytest.raises(RecoveryRequired): manager.run()
    assert not client.orders


def test_low_bnb_is_replenished_before_other_conversions(tmp_path):
    client, manager = setup(tmp_path)
    client.balance['BNB'] = D('.002')
    client.balance['USDC'] = D(30)
    result = manager.run()
    assert client.orders[0].symbol == 'USDCUSDT'
    assert client.orders[0].side == 'sell'
    assert client.orders[1].symbol == 'BNBUSDT'
    assert client.balance['BNB'] >= D('.0199')
    assert result['status'] == 'REBALANCE_COMPLETE'


def test_no_cash_for_bnb_blocks_all_conversions(tmp_path):
    client, manager = setup(tmp_path)
    client.balance['USDT'] = D(0)
    client.balance['BNB'] = D(0)
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert 'restore BNB' in result['error']
    assert not client.orders


def test_ambiguous_order_remains_active_and_is_never_retried(tmp_path):
    client, manager = setup(tmp_path)
    def ambiguous(*args, **kwargs): raise AmbiguousOrderError('unknown', ambiguous=True)
    client.new_limit_order = ambiguous
    manager.executor._resolve_client_order = lambda *args: None
    with pytest.raises(AmbiguousOrderError): manager.run()
    assert manager.store.active()[0]['ambiguous_order'] is True
    with pytest.raises(RecoveryRequired): manager.run()


def test_blocked_and_unpriced_assets_are_not_liquidated(tmp_path):
    client, manager = setup(tmp_path)
    manager.store.block_symbol('OLDUSDT', 'restricted')
    plan = manager.snapshot()
    assert 'OLD' in plan['unpriced']
    assert conversion_path(plan['edges'], 'OLD', 'USDT') is None
    manager.run()
    assert client.balance['OLD'] == 5


def test_subminimum_changes_skip_without_orders(tmp_path):
    client, manager = setup(tmp_path)
    client.balance = {'USDT': D(4), 'USDC': D(96), 'BNB': D('.02')}
    result = manager.run(force=True)
    assert not client.orders
    assert result['skipped']


def test_locked_balance_is_valued_but_not_spent(tmp_path):
    client, manager = setup(tmp_path)
    original = client.total_balances
    client.total_balances = lambda: {**original(), 'BTC': original()['BTC'] + D('.01')}
    manager.run()
    assert all(v >= 0 for v in client.balance.values())


def test_stale_depth_aborts_without_placing(tmp_path):
    client, manager = setup(tmp_path)
    original = client.order_book
    client.order_book = lambda s: replace(original(s), observed_at=time.monotonic() - 10)
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert not client.orders


def test_unconvertible_dust_does_not_leave_one_target_underfunded(tmp_path):
    client, manager = setup(tmp_path)
    client.balance['OLD'] = D(2)  # $4, below the $5 conversion minimum.
    result = manager.run()
    assert result['status'] == 'REBALANCE_COMPLETE'
    assert result['allocation_target_usdt'] == D('90')
    assert client.balance['OLD'] == D(2)
    values = result['after']['values_usdt']
    assert abs(values['USDC'] - 90) < D('.01')
    assert all(client.balance[a] == 0 for a in ('USDT', 'ETH', 'BTC'))


def test_deposited_btc_is_consolidated_without_counting_as_triangle_pnl(tmp_path):
    from binarb.reporting import TradeReports
    client, manager = setup(tmp_path)
    manager.run()
    client.balance['BTC'] = D('.001')
    assert manager.snapshot()['triggered'] is True
    result = manager.run()
    assert result['status'] == 'REBALANCE_COMPLETE'
    values = result['after']['values_usdt']
    assert abs(values['USDC'] - 150) < D('.01')
    assert client.balance['BTC'] == 0
    assert TradeReports(manager.settings.state_dir).summary()['completed_trades'] == 0


def test_account_rejection_is_persisted_without_blind_retry(tmp_path):
    client, manager = setup(tmp_path)
    def reject(*args):
        raise BinanceError('This symbol is not permitted for this account.')
    client.new_limit_order = reject
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert result['orders'][0]['status'] == 'REJECTED'
    assert result['orders'][0]['symbol'] in manager.store.blocked_symbols()
    assert not manager.store.active()


@pytest.mark.parametrize('reject_preflight', [False, True])
def test_restricted_cleanup_does_not_block_other_cleanup_or_target_allocation(tmp_path, reject_preflight):
    client, manager = setup(tmp_path)
    client.prices['XYZUSDT'] = D(2)
    client.pairs['XYZUSDT'] = replace(client.pairs['OLDUSDT'], symbol='XYZUSDT', base='XYZ')
    client.balance['XYZ'] = D(5)
    original_order, original_commission = client.new_limit_order, client.commission_rates
    attempts = []
    def reject_old_order(edge, *args):
        attempts.append(edge.symbol)
        if edge.symbol == 'OLDUSDT':
            raise BinanceError('This symbol is not permitted for this account.')
        # A rejected placement must be persisted before any later submission.
        if edge.symbol == 'XYZUSDT':
            active = manager.store.active()[0]
            if not reject_preflight:
                assert any(o['symbol'] == 'OLDUSDT' and o['status'] == 'REJECTED'
                           for o in active['orders'])
            assert active['conversion_failures']
        return original_order(edge, *args)
    def reject_old_preflight(edge, *args):
        if edge.symbol == 'OLDUSDT':
            raise BinanceError('This symbol is not permitted for this account.')
        return original_commission(edge, *args)
    client.new_limit_order = reject_old_order
    if reject_preflight:
        client.commission_rates = reject_old_preflight
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert 'OLDUSDT' in result['error']
    assert 'OLDUSDT' in manager.store.blocked_symbols()
    assert attempts.count('OLDUSDT') == (0 if reject_preflight else 1)
    assert client.balance['OLD'] == 5
    assert client.balance['XYZ'] == 0
    assert abs(client.balance['USDC'] - 100) < D('.01')
    assert all(client.balance[a] == 0 for a in ('USDT', 'ETH', 'BTC'))
    assert not manager.store.active()


def test_permission_message_with_ambiguous_outcome_still_blocks_all_orders(tmp_path):
    client, manager = setup(tmp_path)
    attempts = []
    def ambiguous(edge, *args):
        attempts.append(edge.symbol)
        raise AmbiguousOrderError('symbol is not permitted for this account', ambiguous=True)
    client.new_limit_order = ambiguous
    manager.executor._resolve_client_order = lambda *args: None
    with pytest.raises(AmbiguousOrderError):
        manager.run()
    assert len(attempts) == 1
    assert manager.store.active()
    assert not manager.store.blocked_symbols()


def test_pause_between_conversions_stops_before_next_order(tmp_path):
    client, manager = setup(tmp_path)
    manager.entry_allowed = lambda: not client.orders
    result = manager.run()
    assert len(client.orders) == 1
    assert result['status'] == 'REBALANCE_PARTIAL'


def test_nonterminal_order_blocks_future_entries(tmp_path):
    client, manager = setup(tmp_path)
    client.new_limit_order = lambda edge, amount, price, tif, cid: Fill(
        'unknown', cid, 'NEW', edge.symbol, edge.side, D(0), D(0))
    manager.executor._wait_terminal = lambda *args: None
    with pytest.raises(AmbiguousOrderError): manager.run()
    assert manager.store.active()


def test_preview_makes_no_orders_and_reports_dust(tmp_path):
    client, manager = setup(tmp_path)
    client.balance['OLD'] = D(2)
    plan = manager.preview()
    assert plan['estimated_available_target_usdt'] == D('90')
    assert 'OLD' in plan['retained_holdings']
    assert not client.orders
    assert not manager.schedule.exists()


def test_rate_limit_reaches_service_backoff_with_durable_partial_state(tmp_path):
    from binarb.errors import RateLimitError
    client, manager = setup(tmp_path)
    def limited(*args): raise RateLimitError('limited', retry_after_s=60)
    client.commission_rates = limited
    with pytest.raises(RateLimitError): manager.run()
    assert not client.orders
    assert not manager.store.active()
    assert len(list(manager.store.archive.glob('*.json'))) == 1


def test_external_commission_cannot_spend_bnb_floor(tmp_path):
    client, manager = setup(tmp_path)
    client.balance['BNB'] = D('.006002')  # $3.001, before the planned fee.
    client.commission_rates = lambda *args: CommissionRates(D('.001'), bnb_eligible=True, multiplier=D('.75'))
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert 'BNB reserve would fall below' in result['error']
    assert not client.orders


def test_changed_commission_asset_is_recorded_before_stopping(tmp_path):
    client, manager = setup(tmp_path)
    original = client.new_limit_order
    def changed(*args):
        fill = original(*args)
        return replace(fill, commissions=(('BNB', D('.00001')),))
    client.new_limit_order = changed
    result = manager.run()
    assert result['status'] == 'REBALANCE_PARTIAL'
    assert 'commission asset changed' in result['error']
    assert len(result['orders']) == 1
    assert result['orders'][0]['filled_volume'] > 0
    assert result['commissions']['BNB'] == D('.00001')


def test_periodic_cleanup_runs_even_when_all_starts_are_within_twenty_percent(tmp_path):
    client, manager = setup(tmp_path)
    manager.run()
    client.balance['OLD'] = D(10)  # Deposit $20, below the relative drift threshold.
    assert manager.snapshot()['triggered'] is False
    assert manager.preview()['cleanup_required'] is True
    result = manager.run()
    assert result['status'] == 'REBALANCE_COMPLETE'
    assert client.balance['OLD'] == 0
    assert result['remaining_cleanup'] == []
    assert client.balance['BNB'] == D('.02')


def test_dry_run_uses_periodic_cadence_without_mutating_live_state(tmp_path):
    client, manager = setup(tmp_path)
    manager.settings = replace(manager.settings, dry_run=True)
    assert manager.due()
    result = manager.run()
    assert result['cleanup_required'] is True
    assert not manager.due()
    assert manager.due(time.time() + 301)
    assert not manager.schedule.exists()
    assert not manager.store.active()
    assert not client.orders


def test_direct_bnb_topup_spends_usdc_at_market_valuation(tmp_path):
    client, manager = setup(tmp_path)
    client.prices['USDCUSDT'] = D('.8')
    client.prices['BNBUSDC'] = D(625)
    client.pairs['BNBUSDC'] = replace(client.pairs['BNBUSDT'], symbol='BNBUSDC', quote='USDC')
    client.balance = {'USDC': D(100), 'BNB': D('.002')}
    result = manager.run(reserve_only=True)
    assert result['status'] == 'REBALANCE_COMPLETE'
    assert len(client.orders) == 1
    assert client.orders[0].symbol == 'BNBUSDC'
    assert client.orders[0].cost == D('11.25')  # $9 deficit / $0.80 per USDC.
    assert client.balance['BNB'] == D('.02')
    assert client.balance['USDC'] == D('88.75')


def test_excess_bnb_converts_to_usdc_but_locked_bnb_does_not_fund_reserve(tmp_path):
    client, manager = setup(tmp_path)
    client.balance = {'USDC': D(100), 'BNB': D('.04')}
    client.total_balances = lambda: {**client.balance, 'BNB': client.balance['BNB'] + D('.03')}
    result = manager.run()
    assert result['status'] == 'REBALANCE_COMPLETE'
    assert client.balance['BNB'] == D('.02')
    assert client.balance['USDC'] == D(110)


def test_usdc_allocation_does_not_require_btc_or_eth_markets(tmp_path):
    client, manager = setup(tmp_path)
    client.balance = {'USDC': D(100), 'BNB': D('.02')}
    for symbol in ('BTCUSDT', 'ETHUSDT'):
        del client.pairs[symbol]
        del client.prices[symbol]
    assert manager.run()['status'] == 'REBALANCE_COMPLETE'
    assert not client.orders
