from decimal import Decimal
import json

from binarb.reporting import TradeReports
from binarb.state import StateStore
from binarb.app import Settings, status_text


def trade(deal_id='one', asset='USDT', pnl='0.008119', **extra):
    return dict(deal_id=deal_id, status='COMPLETE_WITH_DUST', created_at=1,
                route=[asset, 'A', 'B', asset], realized_pnl=pnl,
                orders=[{'filled_volume': '1'}], **extra)


def test_archive_pruning_does_not_erase_pnl_or_duplicate_events(tmp_path):
    store = StateStore(tmp_path / 'state', archive_max_files=1)
    store.finish('one', trade())
    store.finish('two', trade('two', pnl='-0.002'))
    reports = TradeReports(store.directory)
    reports.record(trade())
    assert len(list(store.archive.glob('*.json'))) == 1
    summary = reports.summary()
    assert summary['completed_trades'] == 2
    assert Decimal(summary['realized_pnl_by_asset']['USDT']) == Decimal('0.006119')
    sent = []
    reports.deliver(lambda body: sent.append(body) or True)
    reports.deliver(lambda body: sent.append(body) or True)
    assert len(sent) == 2
    assert 'After recorded commissions' in sent[0]


def test_notifications_retry_without_affecting_trade_state(tmp_path):
    reports = TradeReports(tmp_path / 'state')
    reports.record(trade())
    calls = []
    def unavailable(body):
        calls.append(body)
        raise TimeoutError()
    reports.deliver(unavailable, now=100)
    reports.deliver(unavailable, now=101)
    assert len(calls) == 1
    reports.deliver(lambda body: calls.append(body) or True, now=131)
    assert len(calls) == 2
    assert reports.summary()['completed_trades'] == 1


def test_backfill_is_silent_and_keeps_currency_and_unknown_pnl(tmp_path):
    state = tmp_path / 'state'
    (state / 'archive').mkdir(parents=True)
    (state / 'archive' / 'old.json').write_text(json.dumps(trade(asset='USDC', pnl='-5.003')))
    reports = TradeReports(state)
    reports.import_archives(state)
    reports.record(trade('unknown', asset='ETH', pnl=None))
    reports.record(trade('maintenance', kind='REBALANCE'))
    summary = reports.summary()
    assert summary['realized_pnl_by_asset'] == {'USDC': '-5.003'}
    assert summary['pnl_unknown_trades'] == 1
    assert summary['pnl_usdt_unvalued_trades'] == 2
    sent = []
    reports.deliver(lambda body: sent.append(body) or True)
    assert len(sent) == 1 and 'unknown ETH' in sent[0]
    text = status_text(Settings.load(), summary)
    assert '-5.00300000 USDC' in text
    assert 'Completed triangles: 2' in text


def test_recovery_losses_are_included_and_no_order_rejections_are_excluded(tmp_path):
    reports = TradeReports(tmp_path / 'state')
    loss = trade(pnl='-.02')
    loss['status'] = 'RECOVERED'
    reports.record(loss)
    rejected = trade('rejected', pnl=None)
    rejected['orders'] = [{'status': 'SUBMITTING'}]
    reports.record(rejected)
    assert reports.summary()['realized_pnl_by_asset'] == {'USDT': '-0.02'}
    assert reports.summary()['completed_trades'] == 0
