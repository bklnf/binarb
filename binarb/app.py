from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import logging
import math
import os
import shlex
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

from . import control_plane as cp
from .client import BinanceClient
from .executor import Executor, client_id
from .errors import RateLimitError
from .market_stream import BookTickerStream
from .scanner import (best_size, best_size_detailed, build_edges, discover_triangles,
                      observation_error, route_minimum_start, screen_top_of_book,
                      top_of_book_opportunities)
from .state import StateStore

logger = logging.getLogger(__name__)
LIVE_ACK = "I_ACCEPT_LIVE_TRADING"


def load_env_file(path=".env"):
    env_path = Path(path)
    if not env_path.exists(): return
    for raw in env_path.read_text().splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#") or "=" not in raw: continue
        key, value = raw.split("=", 1)
        if key.strip() not in os.environ:
            tokens = shlex.split(value, comments=True)
            os.environ[key.strip()] = tokens[0] if tokens else ""


def _env(name, default=None):
    raw = os.environ.get(name)
    if raw is None: return default
    tokens = shlex.split(raw, comments=True)
    return tokens[0] if tokens else ""


def _bool(name, default):
    value = _env(name)
    if value is None:
        return default
    if value.lower() not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ValueError(f"{name} must be a boolean")
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    start_currencies: tuple[str, ...]
    excluded_assets: frozenset[str]
    dry_run: bool
    min_net_bps: Decimal
    balance_share: Decimal
    min_size_share: Decimal
    max_slippage_bps: Decimal
    scan_interval_s: float
    ticker_max_age_s: float
    rest_seed_interval_s: float
    state_dir: str
    archive_retention_days: int
    archive_max_files: int
    auto_recover: bool
    scan_log_interval_s: float
    use_bnb_fee_discount: bool
    bnb_replenish_enabled: bool
    bnb_replenish_floor_usdt: Decimal
    bnb_replenish_target_usdt: Decimal
    confirmation_candidates_per_start: int
    confirmation_candidates_total: int
    quote_max_skew_s: float = 0.5

    @classmethod
    def load(cls, *, permit_live=False):
        dry_run = _bool("ARB_DRY_RUN_BINANCE", True)
        settings = cls(
            tuple(dict.fromkeys(item.strip().upper() for item in _env(
                "ARB_START_CURRENCIES_BINANCE",
                "USDT,USDC,FDUSD,BTC,ETH,BNB,EUR,TRY,BRL,JPY,MXN,ZAR"
            ).split(",") if item.strip())),
            frozenset(item.strip().upper() for item in _env(
                "ARB_EXCLUDED_ASSETS_BINANCE", "IDR"
            ).split(",") if item.strip()),
            dry_run,
            Decimal(_env("ARB_MIN_NET_BPS_BINANCE", "10")),
            Decimal(_env("BALANCE_SHARE_TO_BID_BINANCE", "1.00")),
            Decimal(_env("ARB_MIN_SIZE_SHARE_BINANCE", "0.10")),
            Decimal(_env("ARB_MAX_SLIPPAGE_BPS_BINANCE", "15")),
            float(_env("ARB_SCAN_INTERVAL_MS_BINANCE", "100")) / 1000,
            float(_env("ARB_TICKER_MAX_AGE_S_BINANCE", "2")),
            float(_env("ARB_REST_SEED_INTERVAL_S_BINANCE", "15")),
            _env("ARB_STATE_DIR_BINANCE", "data/state"),
            int(_env("ARB_ARCHIVE_RETENTION_DAYS_BINANCE", "30")),
            int(_env("ARB_ARCHIVE_MAX_FILES_BINANCE", "2000")),
            _bool("ARB_AUTO_RECOVER_BINANCE", True),
            float(_env("ARB_SCAN_LOG_INTERVAL_S_BINANCE", "10")),
            _bool("ARB_USE_BNB_FEE_DISCOUNT_BINANCE", False),
            _bool("ARB_BNB_REPLENISH_ENABLED_BINANCE", False),
            Decimal(_env("ARB_BNB_REPLENISH_FLOOR_USDT_BINANCE", "3")),
            Decimal(_env("ARB_BNB_REPLENISH_TARGET_USDT_BINANCE", "10")),
            int(_env("ARB_CONFIRMATION_CANDIDATES_PER_START_BINANCE", "3")),
            int(_env("ARB_CONFIRMATION_CANDIDATES_TOTAL_BINANCE", "12")),
            float(_env("ARB_QUOTE_MAX_SKEW_S_BINANCE", "0.5")),
        )
        for name in ("min_net_bps", "balance_share", "min_size_share", "max_slippage_bps",
                     "bnb_replenish_floor_usdt", "bnb_replenish_target_usdt"):
            if not getattr(settings, name).is_finite():
                raise ValueError(f"{name} must be finite")
        for name in ("scan_interval_s", "ticker_max_age_s", "rest_seed_interval_s",
                     "scan_log_interval_s", "quote_max_skew_s"):
            value = getattr(settings, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if settings.quote_max_skew_s > settings.ticker_max_age_s:
            raise ValueError("quote skew must not exceed ticker maximum age")
        if settings.archive_retention_days <= 0 or settings.archive_max_files <= 0:
            raise ValueError("archive retention limits must be positive")
        if not settings.start_currencies: raise ValueError("start currencies are empty")
        if settings.min_net_bps < 0 or not settings.min_net_bps.is_finite():
            raise ValueError("ARB_MIN_NET_BPS_BINANCE must be non-negative")
        if not Decimal(0) < settings.balance_share <= 1: raise ValueError("balance share must be in (0,1]")
        if not Decimal(0) < settings.min_size_share <= 1: raise ValueError("min size share must be in (0,1]")
        if settings.max_slippage_bps < 0: raise ValueError("max slippage must be non-negative")
        if settings.scan_log_interval_s <= 0: raise ValueError("scan log interval must be positive")
        if settings.bnb_replenish_floor_usdt < 0: raise ValueError("BNB replenish floor must be non-negative")
        if settings.bnb_replenish_target_usdt < settings.bnb_replenish_floor_usdt:
            raise ValueError("BNB replenish target must be at least its floor")
        if settings.confirmation_candidates_per_start <= 0:
            raise ValueError("confirmation candidates per start must be positive")
        if settings.confirmation_candidates_total <= 0:
            raise ValueError("total confirmation candidates must be positive")
        if not dry_run and permit_live and _env("BINANCE_LIVE_ACK") != LIVE_ACK:
            raise RuntimeError(f"live mode requires BINANCE_LIVE_ACK={LIVE_ACK}")
        return settings


def make_client():
    return BinanceClient(_env("BINANCE_API_KEY", ""), _env("BINANCE_API_SECRET", ""),
                         api_root=_env("BINANCE_API_ROOT", "https://api.binance.com"))


def bootstrap(client, settings=None):
    offset = client.sync_time()
    pairs = client.load_pair_metadata()
    client.load_account_fees()
    if settings and settings.use_bnb_fee_discount:
        multiplier = client.configure_bnb_discount(client.tickers(), enabled=True)
        logger.info("BNB fee screening multiplier=%s", multiplier)
    return offset, pairs, client.fees


def maintain_bnb_fee_reserve(client, settings, tickers, balances):
    """Ensure BNB fee capacity before scanning; never run during a triangle."""
    if not settings.use_bnb_fee_discount:
        client.set_bnb_discount_active(False)
        return False
    prices = tickers.get("BNBUSDT")
    if prices is None:
        client.set_bnb_discount_active(False)
        logger.warning("BNB fee reserve unavailable: no BNBUSDT ticker")
        return False
    bnb_value = balances.get("BNB", Decimal(0)) * prices[0]
    if bnb_value >= settings.bnb_replenish_floor_usdt:
        client.set_bnb_discount_active(True)
        return True
    client.set_bnb_discount_active(False)
    if not settings.bnb_replenish_enabled or settings.dry_run:
        logger.warning("BNB fee reserve below floor value_usdt=%s floor_usdt=%s", bnb_value,
                       settings.bnb_replenish_floor_usdt)
        return False
    edge = client.edge("USDT", "BNB", tickers)
    available = balances.get("USDT", Decimal(0))
    budget = min(settings.bnb_replenish_target_usdt - bnb_value, available)
    if edge is None or budget <= 0:
        logger.warning("BNB fee replenish skipped: no usable USDT route or balance")
        return False
    try:
        client.prepare_market_order(edge, budget)
    except ValueError as exc:
        logger.warning("BNB fee replenish skipped budget_usdt=%s: %s", budget, exc)
        return False
    token = f"barb-fee-bnb-{time.time_ns()}"[:36]
    fill = client.new_market_order(edge, budget, token)
    acquired = max(fill.volume - fill.commission("BNB"), Decimal(0))
    logger.warning("BNB fee reserve replenished spent_usdt=%s acquired_bnb=%s target_usdt=%s",
                   fill.cost, acquired, settings.bnb_replenish_target_usdt)
    try:
        from .telegram import send_message
        send_message("💎 BNB fee reserve replenished\n"
                     f"Spent: {fill.cost} USDT\nAcquired: {acquired} BNB\n"
                     f"Target: ${settings.bnb_replenish_target_usdt}")
    except Exception:
        logger.exception("BNB fee replenish notification failed")
    return False


def portfolio_value_usd(client, tickers, balances, *, max_hops=3):
    """Fee-adjusted USDT liquidation value using paths of at most three markets."""
    edges = build_edges(client.pairs, client.fees, tickers)
    adjacency = {}
    for edge in edges.values():
        adjacency.setdefault(edge.source, []).append(edge)
    total, unpriced = Decimal(0), []
    for asset, raw_amount in balances.items():
        amount = Decimal(raw_amount)
        if amount <= 0:
            continue
        if asset == "USDT":
            total += amount
            continue
        frontier = [(asset, amount, frozenset({asset}))]
        best_usdt = Decimal(0)
        for _ in range(max_hops):
            following = []
            for source, source_amount, visited in frontier:
                for edge in adjacency.get(source, ()):
                    if edge.target in visited:
                        continue
                    output = (source_amount / edge.price if edge.side == "buy"
                              else source_amount * edge.price)
                    output *= Decimal(1) - edge.fee
                    if edge.target == "USDT":
                        best_usdt = max(best_usdt, output)
                    else:
                        following.append((edge.target, output, visited | {edge.target}))
            frontier = following
            if not frontier:
                break
        if best_usdt > 0:
            total += best_usdt
        else:
            unpriced.append(asset)
    return total, tuple(sorted(unpriced))


def update_portfolio_runtime(runtime, client, tickers, balances):
    value, unpriced = portfolio_value_usd(client, tickers, balances)
    runtime["total_balance_usd"] = str(value.quantize(Decimal("0.01")))
    runtime["unpriced_asset_count"] = len(unpriced)


def find_best(client, settings, tickers, balances, *, blocked_symbols=frozenset()):
    began = time.monotonic()
    edges = build_edges(client.pairs, client.fees, tickers,
                        blocked_symbols=blocked_symbols,
                        excluded_assets=settings.excluded_assets)
    best, best_score = None, None
    stats = {"tickers": len(tickers), "ticker_candidates": 0, "book_candidates": 0,
             "book_rejections": 0, "triangles": 0, "best_signal_bps": None,
             "best_signal_route": None, "confirmed_candidates": 0,
             "rejection_codes": {}, "quote_rejections": {}, "shortlisted_candidates": 0,
             "depth_requests": 0, "best_book_net_bps": None,
             "balances": {a: str(balances.get(a, 0)) for a in settings.start_currencies}}

    def reject(code):
        stats["rejection_codes"][code] = stats["rejection_codes"].get(code, 0) + 1

    pending = []
    for start in settings.start_currencies:
        cap = balances.get(start, Decimal(0)) * settings.balance_share
        if cap <= 0:
            continue
        routes = discover_triangles(edges, start)
        stats["triangles"] += len(routes)
        signals, raw_best_bps = screen_top_of_book(
            edges, routes, cap, settings.min_net_bps,
            max_age_s=settings.ticker_max_age_s, max_skew_s=settings.quote_max_skew_s,
            diagnostics=stats["quote_rejections"],
        )
        if raw_best_bps is not None and (stats["best_signal_bps"] is None
                                         or raw_best_bps > stats["best_signal_bps"]):
            stats["best_signal_bps"], stats["best_signal_route"] = raw_best_bps, start
        stats["ticker_candidates"] += len(signals)
        funded = []
        for signal in signals:
            minimum = max(cap * settings.min_size_share,
                          route_minimum_start(signal.edges, client.pairs))
            if minimum > cap:
                reject("NO_SIZE_GRID")
                continue
            funded.append((signal, cap, minimum))
        reject_count = max(0, len(funded) - settings.confirmation_candidates_per_start)
        if reject_count:
            stats["rejection_codes"]["SHORTLIST_LIMIT"] = (
                stats["rejection_codes"].get("SHORTLIST_LIMIT", 0) + reject_count)
        pending.extend(funded[:settings.confirmation_candidates_per_start])

    pending.sort(key=lambda item: item[0].net_bps, reverse=True)
    if len(pending) > settings.confirmation_candidates_total:
        stats["rejection_codes"]["SHORTLIST_LIMIT"] = (
            stats["rejection_codes"].get("SHORTLIST_LIMIT", 0)
            + len(pending) - settings.confirmation_candidates_total)
    pending = pending[:settings.confirmation_candidates_total]
    stats["shortlisted_candidates"] = len(pending)
    book_cache = {}
    # One bounded pool per scan; only start threads if a funded signal exists.
    with ThreadPoolExecutor(max_workers=3) as pool:
        for signal, cap, minimum in pending:
            code = observation_error(signal.edges, max_age_s=settings.ticker_max_age_s,
                                     max_skew_s=settings.quote_max_skew_s)
            if code:
                reject(code)
                continue
            decision_started = time.monotonic()
            observed = [edge.observed_at for edge in signal.edges]
            logger.info("ticker candidate route=%s signal_net_bps=%.4f "
                        "oldest_quote_ms=%.3f quote_skew_ms=%.3f",
                        "->".join(signal.route), signal.net_bps,
                        (decision_started - min(observed)) * 1000,
                        (max(observed) - min(observed)) * 1000)
            symbols = {edge.symbol for edge in signal.edges}
            try:
                cached = [book_cache[symbol] for symbol in symbols if symbol in book_cache]
                # Refetch the entire route when reused books would be stale or
                # too far apart from newly requested observations.
                reuse_age = min(settings.ticker_max_age_s, settings.quote_max_skew_s)
                if cached and observation_error(cached, max_age_s=reuse_age,
                                                max_skew_s=settings.quote_max_skew_s):
                    for symbol in symbols:
                        book_cache.pop(symbol, None)
                missing = sorted(symbols - book_cache.keys())
                if missing:
                    stats["depth_requests"] += len(missing)
                    book_cache.update(zip(missing, pool.map(client.order_book, missing)))
                books = {symbol: book_cache[symbol] for symbol in symbols}
                code = observation_error(books.values(), max_age_s=settings.ticker_max_age_s,
                                         max_skew_s=settings.quote_max_skew_s)
                if code:
                    reject("DEPTH_" + code)
                    stats["book_rejections"] += 1
                    continue
                candidate, diagnostic = best_size_detailed(
                    signal.edges, books, client.pairs, minimum, cap, settings.min_net_bps,
                )
                stats["confirmed_candidates"] += 1
            except RateLimitError:
                raise
            except Exception as exc:
                logger.warning("depth check failed route=%s error_type=%s",
                               "->".join(signal.route), type(exc).__name__)
                stats["book_rejections"] += 1
                reject("DEPTH_REQUEST_FAILED")
                continue
            book_bps = diagnostic["best_net_bps"]
            if book_bps is not None and (stats["best_book_net_bps"] is None
                                        or book_bps > stats["best_book_net_bps"]):
                stats["best_book_net_bps"] = book_bps
            code = diagnostic["code"]
            if candidate is not None:
                if abs(signal.net_bps - candidate.net_bps) > settings.max_slippage_bps:
                    code, candidate = "FEED_DISAGREEMENT", None
            elapsed_ms = (time.monotonic() - decision_started) * 1000
            logger.info("opportunity decision route=%s code=%s sizes=%s "
                        "pair_rules=%s insufficient_depth=%s book_unprofitable=%s "
                        "best_book_net_bps=%s decision_ms=%.3f unspent_start=%s residuals=%s",
                        "->".join(signal.route), code, diagnostic["sizes"],
                        diagnostic["PAIR_RULES"], diagnostic["INSUFFICIENT_DEPTH"],
                        diagnostic["BOOK_UNPROFITABLE"], book_bps, elapsed_ms,
                        diagnostic["best_unspent_start"], diagnostic["best_residuals"])
            if candidate is None:
                stats["book_rejections"] += 1
                reject(code)
                continue
            stats["book_candidates"] += 1
            if candidate.route[0] == "USDT":
                score = (1, candidate.profit)
            else:
                valuation = client.edge(candidate.route[0], "USDT", tickers)
                if valuation is None:
                    score = (0, candidate.net_bps)
                else:
                    score = (1, candidate.profit / valuation.price if valuation.side == "buy"
                             else candidate.profit * valuation.price)
            if best_score is None or score > best_score:
                best, best_score = candidate, score
    stats["scan_duration_ms"] = (time.monotonic() - began) * 1000
    return best, stats


def _store(settings):
    return StateStore(settings.state_dir, archive_retention_days=settings.archive_retention_days,
                      archive_max_files=settings.archive_max_files)


def run_once(client, settings, tickers, balances, *, permit_live, runtime=None):
    store = _store(settings)
    if store.active(): raise RuntimeError("unresolved deal blocks new entries")
    should_seed = runtime is None or not runtime.get("_blocked_symbols_seeded")
    seeded = store.seed_blocked_symbols_from_archive() if should_seed else frozenset()
    blocked_symbols = store.blocked_symbols()
    candidate, stats = find_best(client, settings, tickers, balances,
                                 blocked_symbols=blocked_symbols)
    if runtime is not None:
        runtime["total_ticker_candidates"] = int(runtime.get("total_ticker_candidates", 0)) + stats["ticker_candidates"]
        runtime["total_confirmed_candidates"] = int(runtime.get("total_confirmed_candidates", 0)) + stats["confirmed_candidates"]
        runtime["total_book_candidates"] = int(runtime.get("total_book_candidates", 0)) + stats["book_candidates"]
        for field in ("rejection_codes", "quote_rejections"):
            totals = runtime.setdefault("total_" + field, {})
            for code, count in stats[field].items():
                totals[code] = totals.get(code, 0) + count
        runtime["total_depth_requests"] = runtime.get("total_depth_requests", 0) + stats["depth_requests"]
        if stats["ticker_candidates"]:
            runtime["last_candidate_at"] = time.time()
            runtime["last_candidate_bps"] = str(stats["best_signal_bps"])
        runtime.update(stats)
        runtime["last_scan_at"] = time.time()
        runtime["scan_count"] = int(runtime.get("scan_count", 0)) + 1
        runtime["blocked_symbols"] = len(blocked_symbols)
        runtime["_blocked_symbols_seeded"] = True
        if seeded:
            logger.warning("blocked historical account-restricted symbols=%s", ",".join(sorted(seeded)))
    if candidate is None:
        if runtime is not None: runtime["last_decision"] = "NO_FEASIBLE_OPPORTUNITY"
        return None
    logger.warning("selected opportunity route=%s size=%s net_bps=%s profit=%s mode=%s",
                   "->".join(candidate.route), candidate.start_amount, candidate.net_bps,
                   candidate.profit, "DRY" if settings.dry_run else "LIVE")
    if not settings.dry_run and not permit_live: return None
    result = Executor(client, store, dry_run=settings.dry_run,
                      auto_recover=settings.auto_recover, min_net_bps=settings.min_net_bps,
                      max_slippage_bps=settings.max_slippage_bps,
                      balance_share=settings.balance_share,
                      min_size_share=settings.min_size_share,
                      max_quote_age_s=settings.ticker_max_age_s,
                      max_quote_skew_s=settings.quote_max_skew_s,
                      entry_allowed=lambda: cp.read_control()["desired_state"] == cp.RUNNING
                      ).execute(candidate)
    logger.warning("opportunity decision route=%s decision=%s deal_id=%s expected_net_bps=%s "
                   "realized_pnl=%s",
                   "->".join(candidate.route), result.get("status"), result.get("deal_id"),
                   candidate.net_bps, result.get("realized_pnl"))
    if runtime is not None:
        runtime.update(last_decision=result["status"], last_route="->".join(candidate.route),
                       last_net_bps=str(candidate.net_bps))
    return result


def probe(client, settings, *, validate=False):
    offset, pairs, fees = bootstrap(client, settings)
    balances, tickers = client.balances(), client.tickers()
    edges = build_edges(pairs, fees, tickers)
    print(f"time_offset_ms={offset} online_pairs={len(pairs)} fee_pairs={len(fees)} tickers={len(tickers)}")
    print("positive_balances=" + ",".join(f"{a}:{v}" for a, v in sorted(balances.items()) if v > 0))
    for start in settings.start_currencies:
        routes = discover_triangles(edges, start); amount = balances.get(start, Decimal(0)) or Decimal(1)
        signals = top_of_book_opportunities(edges, routes, amount, Decimal("-10000"))
        print(f"start={start} balance={balances.get(start, 0)} triangles={len(routes)} "
              f"best_ticker_bps={signals[0].net_bps if signals else None}")
    if validate:
        for edge in (edge for edge in edges.values() if balances.get(edge.source, 0) > 0):
            amount = balances[edge.source] * settings.balance_share
            try:
                book = client.order_book(edge.symbol)
                price = book.asks[0].price if edge.side == "buy" else book.bids[0].price
                client.new_limit_order(edge, amount, price, "FOK", "barb-probe-fok", test=True)
                client.new_limit_order(edge, amount, price, "IOC", "barb-probe-ioc", test=True)
                client.new_market_order(edge, amount, "barb-probe-market", test=True)
                fee = client.test_commission(edge, amount)
            except ValueError: continue
            print(f"test_orders=accepted policies=FOK,IOC,MARKET symbol={edge.symbol} "
                  f"side={edge.side} input={amount} taker_fee={fee}")
            break
    return 0


def status_text(settings, runtime):
    desired = cp.read_control()["desired_state"]
    state_icon = "🟢" if desired == cp.RUNNING else "🟡"
    mode = "🧪 DRY RUN" if settings.dry_run else "🔴 LIVE"
    total = runtime.get("total_balance_usd", "n/a")
    unpriced = int(runtime.get("unpriced_asset_count", 0) or 0)
    total_text = f"💵 Total balance: ≈ ${total}"
    if unpriced:
        total_text += f" ({unpriced} unpriced assets)"
    discount_text = "💎 BNB fee discount: " + (
        "ON" if runtime.get("bnb_fee_discount_active") else "OFF")
    return "\n".join(("🔺 Binance triangle arb", f"Mode: {mode}",
        f"State: {state_icon} {desired.upper()}",
        total_text, f"💰 Starts: {','.join(settings.start_currencies)}",
        f"📈 Tickers: {runtime.get('tickers', 'n/a')}",
        f"📡 Feed: {runtime.get('connected_groups', 'n/a')}/"
        f"{runtime.get('total_groups', 'n/a')} streams; "
        f"last tick age={runtime.get('last_message_age_s', 'n/a')}s",
        f"🔄 Triangles: {runtime.get('triangles', 'n/a')}",
        f"🎯 Candidates: {runtime.get('ticker_candidates', 'n/a')} this scan / "
        f"{runtime.get('total_ticker_candidates', 0)} total",
        f"🔬 Depth checked: {runtime.get('confirmed_candidates', 'n/a')} this scan / "
        f"{runtime.get('total_confirmed_candidates', 0)} total",
        f"✅ Eligible: {runtime.get('total_book_candidates', 0)} total",
        f"🚫 Rejections (total): {runtime.get('total_rejection_codes', {})}",
        f"⏱ Quote exclusions (total): {runtime.get('total_quote_rejections', {})}",
        discount_text,
        f"🧾 Last decision: {runtime.get('last_decision', 'none')}",
        f"⚠️ Last error: {runtime.get('last_error', 'none')}"))


def _handle_rate_limit(exc, runtime):
    wait_s = min(max(float(exc.retry_after_s or 60), 1), 600)
    runtime.update(last_decision="RATE_LIMIT_BACKOFF",
                   last_error=f"RateLimitError: {exc}", rate_limit_wait_s=wait_s)
    logger.error("Binance rate limit endpoint=%s retry_after_s=%s",
                 exc.endpoint, wait_s)
    time.sleep(wait_s)


def stream_probe(client, *, timeout_s=15):
    bootstrap(client); stream = BookTickerStream(client.pairs); stream.start()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            count = len(stream.snapshot(max_age_s=timeout_s + 1))
            if count >= min(100, len(client.pairs)): break
            time.sleep(.25)
        count = len(stream.snapshot(max_age_s=timeout_s + 1))
        print(f"websocket_quotes={count} connections={len(stream.groups)}")
        return 0 if count else 1
    finally:
        stream.stop()


def order_probe(client, *, execute=False, ack="", max_usdt=Decimal("5.5"),
                recover_btc=Decimal(0)):
    """Exercise Binance order paths with a tightly bounded BTCUSDT round trip."""
    bootstrap(client)
    balances, tickers = client.balances(), client.tickers()
    meta = client.pairs.get("BTCUSDT")
    prices = tickers.get("BTCUSDT")
    if meta is None or prices is None:
        raise RuntimeError("BTCUSDT is not available for the order probe")
    if recover_btc:
        if ack != "I_ACCEPT_MATCHING_ENGINE_PROBE":
            raise RuntimeError("probe recovery requires --ack I_ACCEPT_MATCHING_ENGINE_PROBE")
        sell = client.edge("BTC", "USDT", tickers)
        step = meta.market_step or meta.base_step
        minimum = (((meta.min_notional / sell.price) / step)
                   .to_integral_value(rounding=ROUND_CEILING) * step)
        quantity = max(Decimal(recover_btc), minimum)
        available = balances.get("BTC", Decimal(0))
        if quantity > available:
            raise RuntimeError(f"probe recovery needs {quantity} BTC but only {available} is free")
        sold = client.new_market_order(sell, quantity,
                                       client_id(str(time.time_ns()), "probe-recovery"))
        net_usdt = sold.cost - sold.commission("USDT")
        print(f"probe_recovery status={sold.status} requested_btc={recover_btc} "
              f"executed_btc={sold.volume} received_usdt={net_usdt} "
              f"extra_btc_reconsolidated={max(sold.volume - Decimal(recover_btc), Decimal(0))}")
        return 0
    amount = min(Decimal(max_usdt), balances.get("USDT", Decimal(0)) * Decimal("0.90"))
    amount = client._down(amount, Decimal("0.00000001"))
    if amount < meta.min_notional * Decimal("1.05"):
        raise RuntimeError(f"order probe needs at least {meta.min_notional * Decimal('1.05')} USDT")
    buy = client.edge("USDT", "BTC", tickers)
    if buy is None:
        raise RuntimeError("BTCUSDT buy edge is unavailable")
    book = client.order_book("BTCUSDT")
    non_crossing_price = book.bids[0].price * Decimal("0.95")
    token = str(time.time_ns())
    if not execute:
        client.new_limit_order(buy, amount, non_crossing_price, "FOK",
                               client_id(token, "probe-fok"), test=True)
        client.new_limit_order(buy, amount, non_crossing_price, "IOC",
                               client_id(token, "probe-ioc"), test=True)
        client.new_market_order(buy, amount, client_id(token, "probe-market"), test=True)
        print(f"order_probe=VALIDATED symbol=BTCUSDT policies=FOK,IOC,MARKET budget_usdt={amount}")
        return 0
    if ack != "I_ACCEPT_MATCHING_ENGINE_PROBE":
        raise RuntimeError("executing probe requires --ack I_ACCEPT_MATCHING_ENGINE_PROBE")
    for policy in ("FOK", "IOC"):
        fill = client.new_limit_order(buy, amount, non_crossing_price, policy,
                                      client_id(token, f"probe-{policy.lower()}"))
        print(f"matching_probe policy={policy} status={fill.status} executed_btc={fill.volume} "
              f"spent_usdt={fill.cost}")
        if fill.volume or fill.cost:
            raise RuntimeError(f"unexpected {policy} fill; stop and reconsolidate manually")
    bought = client.new_market_order(buy, amount, client_id(token, "probe-market-buy"))
    acquired = bought.volume - bought.commission("BTC")
    print(f"matching_probe policy=MARKET_BUY status={bought.status} executed_btc={bought.volume} "
          f"spent_usdt={bought.cost}")
    if acquired <= 0:
        raise RuntimeError("market buy produced no reconsolidatable BTC")
    fresh = client.tickers()
    sell = client.edge("BTC", "USDT", fresh)
    if sell is None:
        raise RuntimeError("BTCUSDT sell edge unavailable after market probe")
    sell_amount = acquired
    try:
        client.prepare_market_order(sell, sell_amount)
    except ValueError:
        step = meta.market_step or meta.base_step
        minimum = (((meta.min_notional / sell.price) / step)
                   .to_integral_value(rounding=ROUND_CEILING) * step)
        if minimum - acquired > step or client.balances().get("BTC", Decimal(0)) < minimum:
            raise RuntimeError("probe fill is below sell minimum and cannot be safely reconsolidated")
        sell_amount = minimum
    sold = client.new_market_order(sell, sell_amount, client_id(token, "probe-market-sell"))
    net_usdt = sold.cost - sold.commission("USDT")
    print(f"matching_probe policy=MARKET_SELL status={sold.status} executed_btc={sold.volume} "
          f"received_usdt={net_usdt} extra_btc_reconsolidated="
          f"{max(sold.volume - acquired, Decimal(0))} "
          f"round_trip_cash_delta_usdt={net_usdt - bought.cost}")
    return 0


def main(argv=None):
    load_env_file(); parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command")
    p = sub.add_parser("probe"); p.add_argument("--validate-order", action="store_true")
    op = sub.add_parser("order-probe")
    op.add_argument("--execute", action="store_true")
    op.add_argument("--ack", default="")
    op.add_argument("--max-usdt", type=Decimal, default=Decimal("5.5"))
    op.add_argument("--recover-btc", type=Decimal, default=Decimal(0))
    sub.add_parser("scan-once"); sub.add_parser("stream-probe"); sub.add_parser("run")
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, _env("ARB_LOG_LEVEL", "INFO").upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.load(permit_live=args.command == "run")
    if args.command == "probe": return probe(make_client(), settings, validate=args.validate_order)
    if args.command == "order-probe":
        return order_probe(make_client(), execute=args.execute, ack=args.ack,
                           max_usdt=args.max_usdt, recover_btc=args.recover_btc)
    if args.command == "stream-probe": return stream_probe(make_client())
    if args.command not in {"scan-once", "run"}: parser.print_help(); return 2
    client = make_client(); offset, pairs, fees = bootstrap(client, settings)
    balances, seed = client.balances(), client.tickers()
    total_balances = client.total_balances()
    logger.info("bootstrap complete mode=%s time_offset_ms=%d online_pairs=%d fee_pairs=%d "
                "tickers=%d starts=%s scan_interval_ms=%d log_interval_s=%.1f",
                "DRY_RUN" if settings.dry_run else "LIVE", offset, len(pairs), len(fees),
                len(seed), ",".join(settings.start_currencies),
                int(settings.scan_interval_s * 1000), settings.scan_log_interval_s)
    if args.command == "scan-once":
        print(run_once(client, settings, seed, balances, permit_live=False) or "NO_FEASIBLE_OPPORTUNITY")
        return 0
    stream = BookTickerStream(client.pairs)
    stream.seed(seed, observed_at=min(seed.observed_at.values(), default=time.monotonic()))
    stream.start()
    runtime = {"pairs": len(client.pairs), "last_decision": "STARTED", "scan_count": 0}
    update_portfolio_runtime(runtime, client, seed, total_balances)
    started, last_heartbeat, last_seed, balance_at = time.time(), 0., time.monotonic(), 0.
    last_scan_log, last_control_state = 0., None
    try:
        while True:
            control, now = cp.read_control(), time.monotonic()
            if control["desired_state"] != last_control_state:
                logger.info("control state=%s", control["desired_state"].upper())
                last_control_state = control["desired_state"]
            if control["clear_issued_at"]:
                runtime["last_clear"] = _store(settings).clear_active(); cp.consume_clear(); control = cp.read_control()
            health_tickers = stream.snapshot(max_age_s=settings.ticker_max_age_s)
            runtime.update(stream.health(max_age_s=settings.ticker_max_age_s), tickers=len(health_tickers))
            if now - last_heartbeat >= 2:
                cp.write_heartbeat(control["desired_state"], status_text(settings, runtime), runtime,
                                   started_at=started); last_heartbeat = now
            if control["desired_state"] == cp.PAUSED:
                time.sleep(min(settings.scan_interval_s, 1)); continue
            if now - last_seed >= settings.rest_seed_interval_s:
                seed_started = time.monotonic()
                try:
                    stream.seed(client.tickers(), observed_at=seed_started)
                except RateLimitError as exc:
                    _handle_rate_limit(exc, runtime); continue
                last_seed = now
            refresh_balances = now - balance_at >= 5
            if refresh_balances:
                try:
                    balances, balance_at = client.balances(), now
                    update_portfolio_runtime(runtime, client,
                                             stream.snapshot(max_age_s=settings.ticker_max_age_s),
                                             client.total_balances())
                except RateLimitError as exc:
                    _handle_rate_limit(exc, runtime); continue
            fresh_tickers = stream.snapshot(max_age_s=settings.ticker_max_age_s)
            if refresh_balances:
                bnb_discount_active = maintain_bnb_fee_reserve(client, settings, fresh_tickers, balances)
                runtime["bnb_fee_discount_active"] = bnb_discount_active
                if not bnb_discount_active and settings.bnb_replenish_enabled and not settings.dry_run:
                    balance_at = 0
            try:
                result = run_once(client, settings, fresh_tickers, balances,
                                  permit_live=True, runtime=runtime)
            except RateLimitError as exc:
                _handle_rate_limit(exc, runtime); continue
            if result and not settings.dry_run: balance_at = 0
            if now - last_scan_log >= settings.scan_log_interval_s:
                funded = ",".join(f"{asset}:{balances.get(asset, 0)}"
                                  for asset in settings.start_currencies
                                  if balances.get(asset, 0) > 0)
                logger.info("scan heartbeat mode=%s scans=%d fresh_tickers=%d triangles=%d "
                            "ticker_candidates=%d confirmed_candidates=%d book_candidates=%d "
                            "book_rejections=%d rejection_codes=%s total_ticker_candidates=%d "
                            "stream_groups=%s/%s last_tick_age_s=%s best_signal_bps=%s "
                            "best_start=%s decision=%s balances=%s scan_ms=%.3f "
                            "stale_quotes=%s best_book_net_bps=%s total_rejections=%s "
                            "total_quote_exclusions=%s total_depth_requests=%s",
                            "DRY_RUN" if settings.dry_run else "LIVE", runtime["scan_count"],
                            len(fresh_tickers), runtime.get("triangles", 0),
                            runtime.get("ticker_candidates", 0),
                            runtime.get("confirmed_candidates", 0),
                            runtime.get("book_candidates", 0), runtime.get("book_rejections", 0),
                            runtime.get("rejection_codes", {}),
                            runtime.get("total_ticker_candidates", 0),
                            runtime.get("connected_groups"), runtime.get("total_groups"),
                            runtime.get("last_message_age_s"), runtime.get("best_signal_bps"),
                            runtime.get("best_signal_route"), runtime.get("last_decision"), funded,
                            runtime.get("scan_duration_ms", 0), runtime.get("stale_quotes", 0),
                            runtime.get("best_book_net_bps"), runtime.get("total_rejection_codes", {}),
                            runtime.get("total_quote_rejections", {}), runtime.get("total_depth_requests", 0))
                last_scan_log = now
            time.sleep(settings.scan_interval_s)
    except KeyboardInterrupt: return 0
    except Exception as exc:
        runtime.update(last_decision="ERROR", last_error=f"{type(exc).__name__}: {exc}")
        cp.write_heartbeat(cp.read_control()["desired_state"], status_text(settings, runtime), runtime,
                           started_at=started); logger.exception("service stopped"); return 1
    finally: stream.stop()
