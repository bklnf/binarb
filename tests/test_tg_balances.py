from types import SimpleNamespace
from unittest.mock import Mock
from decimal import Decimal

import pytest

from binarb import tg_gateway as gateway
from binarb.telegram import TelegramPoller


def account_client(monkeypatch, balances):
    prices = {'BTC': Decimal('80000'), 'ETH': Decimal('2000')}
    pairs, tickers = {}, {}
    for row in balances:
        asset = row['asset']
        if asset in {'USDT', 'UNPRICED'}:
            continue
        symbol = asset + 'USDT'
        pairs[symbol] = SimpleNamespace(base=asset, quote='USDT')
        price = prices.get(asset, Decimal(1))
        tickers[symbol] = (price, price)
    client = SimpleNamespace(
        sync_time=Mock(), account=Mock(return_value={"balances": balances}),
        load_pair_metadata=Mock(), pairs=pairs, tickers=Mock(return_value=tickers),
        session=SimpleNamespace(close=Mock()),
    )
    monkeypatch.setattr(gateway, "make_client", lambda: client)
    return client


def test_balances_fetches_fresh_snapshot_including_locked_and_hides_dust(monkeypatch):
    client = account_client(monkeypatch, [
        {"asset": "USDT", "free": "10.50000000", "locked": "2.25000000"},
        {"asset": "ZERO", "free": "0.00000000", "locked": "0"},
        {"asset": "BTC", "free": "0.00000001", "locked": "0.00000000"},
        {"asset": "ETH", "free": "0", "locked": "0.125"},
    ])
    reply = gateway.handle("/balances")
    assert "UTC" in reply and "Binance Spot" in reply
    assert "BTC:" not in reply
    assert "ETH: total 0.125 | available 0 | locked 0.125" in reply
    assert "USDT: total 12.75 | available 10.5 | locked 2.25" in reply
    assert "ZERO" not in reply
    assert "≈ $250.00" in reply
    assert reply.index("ETH:") < reply.index("USDT:")
    client.sync_time.assert_called_once_with()
    client.account.assert_called_once_with(refresh=True)
    client.session.close.assert_called_once_with()


def test_balances_empty_account_and_usage(monkeypatch):
    client = account_client(monkeypatch, [])
    assert gateway.handle("/balances USDT") == "Usage: /balances"
    client.account.assert_not_called()
    assert "No priced Spot holdings worth at least $2." in gateway.handle("/balances")


@pytest.mark.parametrize("operation", ["sync_time", "account", "tickers"])
def test_balances_errors_do_not_expose_signed_request_details(monkeypatch, caplog, operation):
    client = account_client(monkeypatch, [])
    getattr(client, operation).side_effect = RuntimeError("private-signature-or-key")
    reply = gateway.handle("/balances")
    assert "balances unavailable" in reply
    assert "private-signature-or-key" not in reply + caplog.text
    client.session.close.assert_called_once_with()


def test_balances_large_account_is_split_without_dropping_rows(monkeypatch):
    account_client(monkeypatch, [
        {"asset": f"ASSET{i:04}", "free": "0.00000001", "locked": "2.25"}
        for i in range(200)
    ])
    reply = gateway.handle("/balances")
    chunks = list(gateway.reply_chunks(reply))
    assert len(chunks) > 1
    assert all(len(chunk) <= 4000 for chunk in chunks)
    assert "\n".join(chunks) == reply


def test_balances_threshold_uses_unrounded_total_and_counts_unpriced(monkeypatch):
    account_client(monkeypatch, [
        {"asset": "BELOW", "free": "1.999999", "locked": "0"},
        {"asset": "EXACT", "free": "1.25", "locked": ".75"},
        {"asset": "USDT", "free": "2", "locked": "0"},
        {"asset": "UNPRICED", "free": "100", "locked": "0"},
    ])
    reply = gateway.handle('/balances')
    assert 'BELOW:' not in reply
    assert 'EXACT: total 2' in reply and 'USDT: total 2' in reply
    assert 'UNPRICED:' not in reply
    assert 'Unpriced holdings omitted: 1.' in reply


def test_balances_values_indirect_and_inverse_markets(monkeypatch):
    client = account_client(monkeypatch, [
        {"asset": "ALT", "free": "2", "locked": "0"},
        {"asset": "TRY", "free": "120", "locked": "0"},
    ])
    client.pairs = {
        'ALTETH': SimpleNamespace(base='ALT', quote='ETH'),
        'ETHUSDT': SimpleNamespace(base='ETH', quote='USDT'),
        'USDTTRY': SimpleNamespace(base='USDT', quote='TRY'),
    }
    client.tickers.return_value = {
        'ALTETH': (Decimal('.001'), Decimal('.0011')),
        'ETHUSDT': (Decimal(2000), Decimal(2001)),
        'USDTTRY': (Decimal(39), Decimal(40)),
    }
    reply = gateway.handle('/balances')
    assert 'ALT: total 2 | available 2 | locked 0 | ≈ $4.00' in reply
    assert 'TRY: total 120 | available 120 | locked 0 | ≈ $3.00' in reply


def test_balances_command_keeps_chat_filter_and_bot_mention_support(monkeypatch):
    monkeypatch.setenv("TG_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TG_CHAT_ID", "123")
    response = SimpleNamespace(ok=True, json=lambda: {"result": [
        {"update_id": 1, "message": {"chat": {"id": 999}, "date": 100, "text": "/balances"}},
        {"update_id": 2, "message": {"chat": {"id": 123}, "date": 100, "text": "/balances@MyBot"}},
    ]})
    monkeypatch.setattr("binarb.telegram.requests.get", lambda *args, **kwargs: response)
    assert TelegramPoller().poll(timeout=1, min_date=90) == ["/balances"]
