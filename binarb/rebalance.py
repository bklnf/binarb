"""USDC consolidation and BNB fee-reserve maintenance. Planning is read-only; execution is explicit."""
from collections import deque
from decimal import Decimal
import json
import logging
from pathlib import Path
import time
import uuid

from . import control_plane as cp
from .commissions import plan_edges, reference_symbols, commission_amount
from .errors import AmbiguousOrderError, BinanceError, RateLimitError, RecoveryRequired
from .executor import Executor
from .scanner import build_edges, observation_error
from .state import StateStore

D = Decimal
ASSETS = ('USDC',)
logger = logging.getLogger(__name__)


def conversion_path(edges, source, target, max_hops=3):
    if source == target:
        return ()
    queue = deque([(source, (), frozenset({source}))])
    while queue:
        asset, path, visited = queue.popleft()
        choices = sorted((e for e in edges.values() if e.source == asset),
                         key=lambda e: (e.target != target, e.target not in ASSETS, e.symbol))
        for edge in choices:
            if edge.target in visited or (edge.target == 'BNB' and target != 'BNB'):
                continue
            following = (*path, edge)
            if edge.target == target:
                return following
            if len(following) < max_hops:
                queue.append((edge.target, following, visited | {edge.target}))
    return None


def path_value(path, amount):
    for edge in path:
        amount = amount / edge.price if edge.side == 'buy' else amount * edge.price
    return amount


def allocation(client, settings, balances, totals, tickers, blocked=()):
    # Gross bid/ask valuation in USDT; fills and fees determine the final allocation.
    edges = build_edges(client.pairs, {s: D(0) for s in client.pairs}, tickers,
                        blocked_symbols=blocked, excluded_assets=settings.excluded_assets)
    values, units, paths, unpriced = {}, {}, {}, []
    for asset in sorted(set(totals) | set(ASSETS) | {'BNB'}):
        path = conversion_path(edges, asset, 'USDT')
        if path is None:
            if totals.get(asset, D(0)) > 0:
                unpriced.append(asset)
            continue
        paths[asset] = path
        units[asset] = path_value(path, D(1))
        values[asset] = totals.get(asset, D(0)) * units[asset]
    if any(a not in units for a in (*ASSETS, 'BNB')):
        raise ValueError('missing valuation market for allocation assets or BNB')
    total = sum(values.values(), D(0))
    reserve = settings.bnb_replenish_target_usdt
    target = max(D(0), total - reserve)
    deviations = {a: (abs(values.get(a, D(0)) - target) / target if target else D(0))
                  for a in ASSETS}
    return {'total_usdt': total, 'reserve_usdt': reserve, 'target_usdt': target,
            'values_usdt': values, 'unit_usdt': units, 'unpriced': unpriced,
            'deviations': deviations,
            'triggered': target > 0 and any(v > settings.rebalance_threshold for v in deviations.values()),
            'free': balances, 'paths': paths, 'edges': edges}


def public_plan(snapshot):
    return {k: v for k, v in snapshot.items() if k not in {'paths', 'edges'}}


class Rebalancer:
    def __init__(self, client, settings, *, entry_allowed=None):
        self.client, self.settings = client, settings
        self.store = StateStore(settings.state_dir,
                               archive_retention_days=settings.archive_retention_days,
                               archive_max_files=settings.archive_max_files)
        self.entry_allowed = entry_allowed or (lambda: cp.read_control()['desired_state'] == cp.RUNNING)
        self.schedule = Path(settings.state_dir).parent / 'rebalance.json'
        self.executor = Executor(client, self.store, dry_run=False)
        self._dry_checked_at = None

    def snapshot(self):
        balances = self.client.balances()
        totals = self.client.total_balances()
        tickers = self.client.tickers()
        stamps = getattr(tickers, 'observed_at', {})
        if not stamps or time.monotonic() - min(stamps.values()) > self.settings.ticker_max_age_s:
            raise ValueError('rebalance valuation quotes are stale')
        return allocation(self.client, self.settings, balances, totals, tickers,
                          self.store.blocked_symbols())

    def due(self, now=None):
        now = time.time() if now is None else now
        if self.settings.dry_run and self._dry_checked_at is not None:
            return now - self._dry_checked_at >= self.settings.rebalance_interval_s
        try:
            last = json.loads(self.schedule.read_text())['checked_at']
        except FileNotFoundError:
            return True
        return now - last >= self.settings.rebalance_interval_s

    def _precheck(self, snapshot, source, target, amount):
        path = conversion_path(snapshot['edges'], source, target)
        if path is None:
            return None, 'no permitted route'
        projected = amount
        for edge in path:
            try:
                params = self.client.prepare_limit_order(edge, projected, edge.price)
            except ValueError as exc:
                return None, str(exc)
            # Propagate rounded output, with an allowance for received-asset fees.
            quantity = D(params['quantity'])
            projected = (quantity if edge.side == 'buy' else quantity * D(params['price'])) * D('.998')
        return path, None

    def cleanup_plan(self, snapshot):
        """Executable non-start holdings, with the BNB reserve left intact."""
        conversions, retained = [], {}
        for asset, amount in snapshot['free'].items():
            if asset in ASSETS or asset not in snapshot['unit_usdt'] or amount <= 0:
                continue
            if asset == 'BNB':
                amount = max(D(0), amount - self.settings.bnb_replenish_target_usdt / snapshot['unit_usdt'][asset])
            if amount <= 0:
                continue
            path, reason = self._precheck(snapshot, asset, 'USDC', amount)
            if reason:
                retained[asset] = reason
            else:
                conversions.append({'source': asset, 'amount': amount, 'target': 'USDC',
                                    'symbols': [e.symbol for e in path],
                                    'estimated_usdt': amount * snapshot['unit_usdt'][asset]})
        return conversions, retained

    def preview(self):
        snapshot = self.snapshot()
        result = public_plan(snapshot)
        conversions, retained = self.cleanup_plan(snapshot)
        accessible = sum(snapshot['free'].get(a, D(0)) * snapshot['unit_usdt'][a] for a in ASSETS)
        accessible += sum((item['estimated_usdt'] for item in conversions), D(0))
        accessible -= max(D(0), self.settings.bnb_replenish_target_usdt - snapshot['values_usdt']['BNB'])
        result.update(consolidations=conversions, retained_holdings=retained,
                      cleanup_required=bool(conversions),
                      estimated_available_target_usdt=max(D(0), accessible))
        return result

    def _record_restricted_conversion(self, state, edge, exc, *, order_rejected=False):
        # Only a definitive account-permission rejection is safe to skip.
        # Unknown placement outcomes must still stop all subsequent orders.
        if (isinstance(exc, (AmbiguousOrderError, RateLimitError)) or exc.ambiguous
                or 'symbol is not permitted for this account' not in str(exc).lower()):
            return False
        self.executor._block_if_account_restricted(state, edge, exc)
        if order_rejected:
            state['orders'][-1]['status'] = 'REJECTED'
        detail = f'{edge.symbol}: {exc}'
        state.setdefault('conversion_failures', []).append(detail)
        state['error'] = '; '.join(state['conversion_failures'])
        self.store.save(state['deal_id'], state)
        logger.warning('allocation conversion skipped; continuing unrelated conversions: %s', detail)
        return True

    def _convert(self, source, target, amount, state):
        snapshot = self.snapshot()
        path, reason = self._precheck(snapshot, source, target, amount)
        if reason:
            state['skipped'].append(f'{source}->{target}: {reason}')
            return False
        for edge in path:
            if not self.entry_allowed():
                raise RecoveryRequired('operator paused allocation maintenance')
            if edge.symbol in self.store.blocked_symbols():
                raise RecoveryRequired(f'{edge.symbol} is blocked')
            balances = self.client.balances()
            amount = min(amount, balances.get(edge.source, D(0)))
            try:
                profile = self.client.commission_rates(edge, amount)
            except BinanceError as exc:
                if self._record_restricted_conversion(state, edge, exc):
                    return False
                raise
            symbols = reference_symbols((edge,), (profile,), self.client.pairs, balances=balances,
                                        blocked_symbols=self.store.blocked_symbols(),
                                        excluded_assets=self.settings.excluded_assets)
            # At most two books for a direct conversion. Fetch them together.
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=4) as pool:
                names = sorted(symbols)
                books = dict(zip(names, pool.map(self.client.order_book, names)))
            if observation_error(books.values(), max_age_s=self.settings.ticker_max_age_s,
                                 max_skew_s=self.settings.quote_max_skew_s):
                raise RecoveryRequired('rebalance depth is stale or incoherent')
            fresh = plan_edges((edge,), (profile,), books, self.client.pairs, balances)[0]
            if abs(fresh.price / edge.price - 1) * 10000 > self.settings.max_slippage_bps:
                raise RecoveryRequired('rebalance price moved beyond configured limit')
            bnb_path = conversion_path(snapshot['edges'], 'BNB', 'USDT')
            bnb_value = path_value(bnb_path, D(1))
            if fresh.source == 'BNB':
                reserve = self.settings.bnb_replenish_target_usdt / bnb_value
                amount = min(amount, max(D(0), balances.get('BNB', D(0)) - reserve))
            if fresh.fee_asset == 'BNB' and 'BNB' not in {fresh.source, fresh.target}:
                gross = path_value((fresh,), amount)
                remaining_bnb = balances.get('BNB', D(0)) - commission_amount(fresh, gross)
                if remaining_bnb * bnb_value < self.settings.bnb_replenish_floor_usdt:
                    raise RecoveryRequired('BNB reserve would fall below its floor')
            self.client.prepare_limit_order(fresh, amount, fresh.price)
            if not self.entry_allowed():
                raise RecoveryRequired('operator paused before rebalance order')
            # Persist intent and resolve ambiguous transport outcomes by client ID.
            try:
                fill = self.executor._place_and_resolve(
                    state['deal_id'], f"reb{len(state['orders'])}", fresh, amount, state,
                    policy='FOK', price=fresh.price)
            except BinanceError as exc:
                if self._record_restricted_conversion(state, fresh, exc, order_rejected=True):
                    return False
                raise
            spent = (fill.cost if fresh.side == 'buy' else fill.volume) + fill.commission(fresh.source)
            output = (fill.volume if fresh.side == 'buy' else fill.cost) - fill.commission(fresh.target)
            self.executor._record_commissions(state, fill, fresh)
            state['orders'][-1].update(input_spent=spent, net_output=output)
            self.store.save(state['deal_id'], state)
            if spent > amount:
                raise RecoveryRequired('rebalance fill spent beyond its persisted budget')
            if any(value > 0 and asset != fresh.fee_asset for asset, value in fill.commissions):
                raise RecoveryRequired('rebalance commission asset changed after preflight')
            if output <= 0:
                raise RecoveryRequired('rebalance FOK did not fill')
            if not self.executor._await_credit(fresh.target, balances.get(fresh.target, D(0)), output):
                raise AmbiguousOrderError('rebalance fill recorded but balance credit not settled')
            amount = output
        return True

    def run(self, *, force=False, reserve_only=False):
        if self.store.active():
            raise RecoveryRequired('unresolved deal blocks rebalancing')
        snapshot = self.snapshot()
        initial = not self.schedule.exists()
        cleanup, retained = self.cleanup_plan(snapshot)
        needed = force or initial or snapshot['triggered'] or bool(cleanup)
        low_bnb = snapshot['values_usdt'].get('BNB', D(0)) < self.settings.bnb_replenish_floor_usdt
        if self.settings.dry_run:
            self._dry_checked_at = time.time()
            result = {'status': 'REBALANCE_DRY_RUN', 'plan': public_plan(snapshot),
                      'triggered': needed or low_bnb, 'cleanup_required': bool(cleanup),
                      'consolidations': cleanup, 'retained_holdings': retained}
            logger.info('allocation dry-run triggered=%s cleanup_assets=%s target_usdt=%s retained=%d',
                        result['triggered'], [item['source'] for item in cleanup],
                        snapshot['target_usdt'], len(retained))
            return result
        if not self.entry_allowed():
            return {'status': 'REBALANCE_PAUSED'}
        if not reserve_only:
            cp._write(self.schedule, {'checked_at': time.time()})
        if not needed and not low_bnb:
            return {'status': 'REBALANCE_BALANCED', 'plan': public_plan(snapshot)}
        state = {'deal_id': str(uuid.uuid4()), 'kind': 'REBALANCE', 'status': 'REBALANCE_INTENT',
                 'created_at': time.time(), 'orders': [], 'inventory': {}, 'skipped': [],
                 'before': public_plan(snapshot)}
        self.store.save(state['deal_id'], state)
        rate_limit = None
        try:
            # Restore the fee reserve first. No other conversion may spend it.
            if low_bnb:
                edge = conversion_path(snapshot['edges'], 'USDC', 'BNB')
                minimum = (self.client.pairs[edge[0].symbol].min_notional * D('1.02')
                           if edge and len(edge) == 1 else D('5.10') / snapshot['unit_usdt']['USDC'])
                budget = max(minimum, (self.settings.bnb_replenish_target_usdt - snapshot['values_usdt']['BNB'])
                             / snapshot['unit_usdt']['USDC'])
                if snapshot['free'].get('USDC', D(0)) < budget:
                    raise RecoveryRequired('insufficient free USDC to restore BNB reserve')
                self._convert('USDC', 'BNB', budget, state)
                snapshot = self.snapshot()
                if snapshot['values_usdt']['BNB'] < self.settings.bnb_replenish_floor_usdt:
                    raise RecoveryRequired('BNB reserve remains below floor')
            if needed and not reserve_only:
                conversions, retained = self.cleanup_plan(snapshot)
                state['skipped'].extend(f'{asset}->USDC: {reason}'
                                        for asset, reason in retained.items())
                # Recompute each holding after earlier conversions; intermediate
                # currencies and fee deductions may have changed its free balance.
                # Do not refresh account/tickers separately for every dust holding.
                for asset in sorted(item['source'] for item in conversions):
                    snapshot = self.snapshot()
                    conversions, retained = self.cleanup_plan(snapshot)
                    if asset in retained:
                        state['skipped'].append(f'{asset}->USDC: {retained[asset]}')
                    for item in conversions:
                        if item['source'] == asset:
                            self._convert(asset, 'USDC', item['amount'], state)
                snapshot = self.snapshot()
                state['allocation_target_usdt'] = (snapshot['free'].get('USDC', D(0))
                                                   * snapshot['unit_usdt']['USDC'])
            after = self.snapshot()
            remaining_cleanup, _ = self.cleanup_plan(after)
            state['after'] = public_plan(after)
            state['remaining_cleanup'] = remaining_cleanup
            state['status'] = ('REBALANCE_PARTIAL' if state.get('conversion_failures') or
                              ((state['after']['triggered'] or remaining_cleanup) and not reserve_only)
                               else 'REBALANCE_COMPLETE')
        except AmbiguousOrderError as exc:
            state.update(status='RECOVERY_REQUIRED', ambiguous_order=True, error=str(exc))
            self.store.save(state['deal_id'], state)
            raise
        except Exception as exc:
            # Unknown local/transport failures must never retire an active order.
            if state['orders'] and state['orders'][-1]['status'] == 'SUBMITTING':
                if isinstance(exc, BinanceError) and not exc.ambiguous:
                    state['orders'][-1]['status'] = 'REJECTED'
                else:
                    state.update(status='RECOVERY_REQUIRED', error=str(exc))
                    self.store.save(state['deal_id'], state)
                    raise
            state.update(status='REBALANCE_PARTIAL', error=f'{type(exc).__name__}: {exc}')
            if isinstance(exc, RateLimitError):
                rate_limit = exc
        state['completed_at'] = time.time()
        self.store.finish(state['deal_id'], state)
        logger.warning('allocation maintenance status=%s deal_id=%s orders=%d error=%s',
                       state['status'], state['deal_id'], len(state['orders']), state.get('error'))
        if rate_limit is not None:
            raise rate_limit
        return state
