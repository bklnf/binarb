import pytest
from decimal import Decimal

from binarb.app import LIVE_ACK, Settings, portfolio_value_usd, status_text
from binarb import control_plane as cp
from binarb.models import PairMeta
from binarb.tg_gateway import handle


def test_live_mode_requires_separate_ack(monkeypatch):
    monkeypatch.setenv("ARB_DRY_RUN_BINANCE", "false")
    monkeypatch.delenv("BINANCE_LIVE_ACK", raising=False)
    with pytest.raises(RuntimeError): Settings.load(permit_live=True)
    monkeypatch.setenv("BINANCE_LIVE_ACK", LIVE_ACK)
    assert Settings.load(permit_live=True).dry_run is False


def test_operator_pause_and_resume(tmp_path, monkeypatch):
    monkeypatch.setenv("ARB_CONTROL_ROOT_BINANCE", str(tmp_path))
    assert handle("/barb_stop") == "🟡 Binance arb: pause requested; in-flight recovery continues."
    assert cp.read_control()["desired_state"] == cp.PAUSED
    assert handle("/barb_start") == "🟢 Binance arb: resume requested."
    assert cp.read_control()["desired_state"] == cp.RUNNING


def test_heartbeat_serializes_decimal_runtime_metrics(tmp_path, monkeypatch):
    monkeypatch.setenv("ARB_CONTROL_ROOT_BINANCE", str(tmp_path))
    cp.write_heartbeat(cp.RUNNING, "healthy", {"best_signal_bps": Decimal("-1.25")})
    assert cp.read_heartbeat()["runtime"]["best_signal_bps"] == "-1.25"


def _meta(symbol, base, quote):
    return PairMeta(symbol, base, quote, Decimal(".001"), Decimal(".001"),
                    Decimal("100000"), Decimal("1"), None, 8,
                    Decimal(".001"), Decimal(".001"), Decimal("100000"))


def test_portfolio_usd_value_routes_assets_and_reports_unpriced():
    class Client:
        pairs = {"AETH": _meta("AETH", "A", "ETH"),
                 "ETHUSDT": _meta("ETHUSDT", "ETH", "USDT")}
        fees = {"AETH": Decimal(0), "ETHUSDT": Decimal(0)}
    value, unpriced = portfolio_value_usd(
        Client(), {"AETH": (Decimal("2"), Decimal("2.1")),
                   "ETHUSDT": (Decimal("100"), Decimal("101"))},
        {"USDT": Decimal("10"), "A": Decimal("2"), "ZZZ": Decimal("3")},
    )
    assert value == Decimal("410")
    assert unpriced == ("ZZZ",)


def test_telegram_status_includes_total_usd(tmp_path, monkeypatch):
    monkeypatch.setenv("ARB_CONTROL_ROOT_BINANCE", str(tmp_path))
    cp.write_control(cp.RUNNING)
    text = status_text(Settings.load(), {"total_balance_usd": "123.45",
        "unpriced_asset_count": 2})
    assert "💵 Total balance: ≈ $123.45 (2 unpriced assets)" in text
