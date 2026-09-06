from __future__ import annotations

import hashlib
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

import requests

from .errors import AmbiguousOrderError, BinanceError, RecoveryRequired
from .scanner import best_size, observation_error, route_minimum_start


logger = logging.getLogger(__name__)


def client_id(deal_id: str, leg: str) -> str:
    digest = hashlib.sha256(f"{deal_id}:{leg}".encode()).hexdigest()[:20]
    return f"barb-{leg[:3]}-{digest}"[:36]


class Executor:
    def __init__(self, client, state_store, *, dry_run=True, poll_attempts=20,
                 poll_interval_s=.1, settlement_attempts=20, settlement_interval_s=.1,
                 auto_recover=True, min_net_bps=Decimal(0), max_slippage_bps=Decimal(15),
                 balance_share=Decimal(1), min_size_share=Decimal("0.10"),
                 max_quote_age_s=2.0, max_quote_skew_s=0.5, entry_allowed=None):
        self.client, self.state_store, self.dry_run = client, state_store, dry_run
        self.poll_attempts, self.poll_interval_s = poll_attempts, poll_interval_s
        self.settlement_attempts, self.settlement_interval_s = settlement_attempts, settlement_interval_s
        self.auto_recover = auto_recover
        self.min_net_bps, self.max_slippage_bps = Decimal(min_net_bps), Decimal(max_slippage_bps)
        self.balance_share = Decimal(balance_share)
        self.min_size_share = Decimal(min_size_share)
        self.max_quote_age_s, self.max_quote_skew_s = max_quote_age_s, max_quote_skew_s
        self.entry_allowed = entry_allowed or (lambda: True)

    def execute(self, opportunity):
        deal_id = str(uuid.uuid4())
        state = {"deal_id": deal_id, "status": "DRY_RUN" if self.dry_run else "INTENT_WRITTEN",
                 "created_at": time.time(), "route": opportunity.route,
                 "start_amount": opportunity.start_amount,
                 "expected_end_amount": opportunity.end_amount,
                 "expected_net_bps": opportunity.net_bps, "orders": [], "inventory": {}}
        if self.dry_run:
            logger.info("execution decision=DRY_RUN deal_id=%s route=%s size=%s expected_net_bps=%s",
                        deal_id, "->".join(opportunity.route), opportunity.start_amount,
                        opportunity.net_bps)
            return state
        logger.warning("execution intent deal_id=%s route=%s size=%s expected_net_bps=%s",
                       deal_id, "->".join(opportunity.route), opportunity.start_amount,
                       opportunity.net_bps)
        self.state_store.save(deal_id, state)
        amount = opportunity.start_amount
        try:
            fresh_balances = self.client.balances()
            # Eligibility is not proof of an available commission reserve.
            # BNB routes may consume that reserve, so reprice them at full fees.
            if hasattr(self.client, "bnb_discount_active"):
                self.client.bnb_discount_active = (
                    self.client.bnb_discount_active
                    and fresh_balances.get("BNB", Decimal(0)) > 0
                    and "BNB" not in opportunity.route)
            # Exact side-specific fee probes are non-executing and also verify
            # commission configuration immediately before exposure is created.
            # A successful order/test is not proof that a live order is
            # permitted for a regionally restricted symbol.
            exact_edges_list, projected = [], amount
            for edge in opportunity.edges:
                try:
                    fee = self.client.test_commission(edge, projected)
                except BinanceError as exc:
                    self._block_if_account_restricted(state, edge, exc)
                    raise
                exact = type(edge)(edge.source, edge.target, edge.symbol, edge.side,
                                   edge.price, fee)
                exact_edges_list.append(exact)
                projected = (projected / edge.price if edge.side == "buy"
                             else projected * edge.price) * (Decimal(1) - fee)
            exact_edges = tuple(exact_edges_list)
            symbols = sorted({edge.symbol for edge in exact_edges})
            with ThreadPoolExecutor(max_workers=min(3, len(symbols))) as pool:
                books = dict(zip(symbols, pool.map(self.client.order_book, symbols)))
            fresh_available = (fresh_balances.get(opportunity.route[0], Decimal(0))
                               * self.balance_share)
            minimum = max(fresh_available * self.min_size_share,
                          route_minimum_start(exact_edges, self.client.pairs))
            repriced = best_size(exact_edges, books, self.client.pairs, minimum,
                                 fresh_available, self.min_net_bps)
            quote_error = observation_error(
                books.values(), max_age_s=self.max_quote_age_s, max_skew_s=self.max_quote_skew_s)
            if (quote_error or not self.entry_allowed() or repriced is None
                    or repriced.net_bps < self.min_net_bps or
                    abs(opportunity.net_bps - repriced.net_bps) > self.max_slippage_bps):
                state.update(status="REPRICE_UNPROFITABLE",
                             rejection_reason=quote_error or ("OPERATOR_PAUSED"
                                 if not self.entry_allowed() else "REPRICE_UNPROFITABLE"),
                             repriced_net_bps=repriced.net_bps if repriced else None,
                             realized_pnl=Decimal(0), pnl_basis="NO_ORDERS")
                self.state_store.finish(deal_id, state)
                logger.warning("execution decision=REPRICE_UNPROFITABLE deal_id=%s route=%s "
                               "signal_net_bps=%s fresh_net_bps=%s",
                               deal_id, "->".join(opportunity.route), opportunity.net_bps,
                               repriced.net_bps if repriced else None)
                return state
            original_size = amount
            amount = repriced.start_amount
            state.update(start_amount=amount, expected_end_amount=repriced.end_amount,
                         expected_net_bps=repriced.net_bps, resized_from=original_size,
                         resized_at=time.time(), fresh_available=fresh_available,
                         projected_unspent_start=repriced.unspent_start,
                         projected_residuals=dict(repriced.residuals))
            self.state_store.save(deal_id, state)
            logger.info("execution authorized deal_id=%s route=%s fresh_net_bps=%s",
                        deal_id, "->".join(opportunity.route), repriced.net_bps)
            for index, edge in enumerate(exact_edges, 1):
                before = fresh_balances if index == 1 else self.client.balances()
                spent, output = self._run_leg(deal_id, index, edge, amount, state,
                                               entry_books=books if index == 1 else None)
                if output <= 0 or spent <= 0:
                    raise RecoveryRequired(f"leg {index} has no confirmed positive fill")
                state["status"] = f"LEG_{index}_FILLED"
                self.state_store.save(deal_id, state)
                logger.warning("leg complete deal_id=%s leg=%d symbol=%s side=%s "
                               "input_budget=%s input_spent=%s net_output=%s",
                               deal_id, index, edge.symbol, edge.side, amount, spent, output)
                if not self._await_credit(edge.target, before.get(edge.target, Decimal(0)), output):
                    raise RecoveryRequired(f"leg {index} balance credit did not settle")
                amount = output
            if not self._recover_to_start(deal_id, state, opportunity.route[0]):
                raise RecoveryRequired("residual inventory could not be flattened")
            end = Decimal(str(state["inventory"].get(opportunity.route[0], amount)))
            gross_pnl = end - Decimal(str(state["actual_start_spent"]))
            external_fee_value, unvalued_fees = self._external_commission_value(
                state, opportunity.route[0])
            state.update(status="COMPLETE_WITH_DUST" if state.get("residual_dust") else "COMPLETE",
                         end_amount=end, realized_pnl_before_external_commissions=gross_pnl,
                         external_commission_value_in_start=external_fee_value,
                         unvalued_external_commissions=unvalued_fees,
                         realized_pnl=(None if unvalued_fees else gross_pnl - external_fee_value),
                         pnl_basis="CASH_FLOW_DUST_UNVALUED",
                         completed_at=time.time())
            self.state_store.finish(deal_id, state)
            logger.warning("execution complete deal_id=%s route=%s status=%s start_spent=%s "
                           "end_amount=%s realized_pnl=%s",
                           deal_id, "->".join(opportunity.route), state["status"],
                           state["actual_start_spent"], end, state["realized_pnl"])
            return state
        except AmbiguousOrderError as exc:
            state.update(status="RECOVERY_REQUIRED", error=str(exc), ambiguous_order=True)
            self.state_store.save(deal_id, state)
            logger.exception("execution ambiguous deal_id=%s route=%s recovery_required=true",
                             deal_id, "->".join(opportunity.route))
            raise
        except Exception as exc:
            state.update(status="RECOVERING" if self.auto_recover else "RECOVERY_REQUIRED",
                         error=f"{type(exc).__name__}: {exc}")
            self.state_store.save(deal_id, state)
            if (self.auto_recover and not state.get("recovery_attempted")
                    and self._recover_to_start(deal_id, state, opportunity.route[0])):
                realized = None
                if state.get("actual_start_spent") is not None:
                    end = Decimal(str(state.get("inventory", {}).get(
                        opportunity.route[0], 0)))
                    gross = end - Decimal(str(state["actual_start_spent"]))
                    external_fee_value, unvalued_fees = self._external_commission_value(
                        state, opportunity.route[0])
                    realized = None if unvalued_fees else gross - external_fee_value
                    state.update(end_amount=end,
                                 realized_pnl_before_external_commissions=gross,
                                 external_commission_value_in_start=external_fee_value,
                                 unvalued_external_commissions=unvalued_fees,
                                 realized_pnl=realized)
                state.update(status="RECOVERED", completed_at=time.time())
                if not state.get("orders"):
                    state.update(realized_pnl=Decimal(0), pnl_basis="NO_ORDERS")
                else:
                    state["pnl_basis"] = "CASH_FLOW_DUST_UNVALUED"
                self.state_store.finish(deal_id, state)
                logger.exception("execution failed and recovered deal_id=%s route=%s "
                                 "realized_pnl=%s",
                                 deal_id, "->".join(opportunity.route), realized)
            else:
                state["status"] = "RECOVERY_REQUIRED"
                self.state_store.save(deal_id, state)
                logger.exception("execution failed recovery_required deal_id=%s route=%s",
                                 deal_id, "->".join(opportunity.route))
            raise RecoveryRequired(f"deal {deal_id}: {state['status']}: {exc}") from exc

    def _run_leg(self, deal_id, leg, edge, amount, state, *, entry_books=None):
        remaining, spent_total, output_total = Decimal(amount), Decimal(0), Decimal(0)
        for policy in ("FOK", "IOC", "MARKET"):
            if remaining <= 0:
                break
            price = None
            try:
                if policy == "MARKET":
                    self.client.prepare_market_order(edge, remaining)
                else:
                    book = self.client.order_book(edge.symbol)
                    if observation_error((book,), max_age_s=self.max_quote_age_s,
                                         max_skew_s=self.max_quote_skew_s):
                        raise RecoveryRequired("leg book is stale or missing its observation time")
                    price = book.asks[0].price if edge.side == "buy" else book.bids[0].price
                    self.client.prepare_limit_order(edge, remaining, price)
            except ValueError as exc:
                logger.info("leg attempt skipped deal_id=%s leg=%s policy=%s symbol=%s "
                            "remaining=%s reason=%s", deal_id, leg, policy, edge.symbol,
                            remaining, exc)
                continue
            if entry_books is not None and spent_total == 0:
                code = observation_error(entry_books.values(), max_age_s=self.max_quote_age_s,
                                         max_skew_s=self.max_quote_skew_s)
                if code or not self.entry_allowed():
                    raise RecoveryRequired(code or "operator paused before entry")
            try:
                fill = self._place_and_resolve(deal_id, f"{leg}-{policy.lower()}", edge,
                                               remaining, state, policy=policy, price=price)
            except BinanceError as exc:
                self._block_if_account_restricted(state, edge, exc)
                raise
            output = max((fill.volume if edge.side == "buy" else fill.cost)
                         - fill.commission(edge.target), Decimal(0))
            spent = ((fill.cost if edge.side == "buy" else fill.volume)
                     + fill.commission(edge.source))
            spent_total += spent
            output_total += output
            remaining = max(Decimal(amount) - spent_total, Decimal(0))
            state["orders"][-1].update(input_spent=spent, net_output=output,
                                        input_remaining=remaining)
            self._record_commissions(state, fill, edge)
            # Record every confirmed attempt before trying another policy.
            # A later rejection must not hide an earlier partial fill.
            self._update_inventory(state, edge, spent, output)
            if leg == 1:
                state["actual_start_spent"] = (
                    Decimal(str(state.get("actual_start_spent", 0))) + spent)
            self.state_store.save(deal_id, state)
            if spent_total > Decimal(amount):
                raise RecoveryRequired(f"leg {leg} {policy} spent beyond persisted budget")
            logger.warning("leg attempt deal_id=%s leg=%s policy=%s symbol=%s status=%s "
                           "input_spent=%s net_output=%s remaining=%s",
                           deal_id, leg, policy, edge.symbol, fill.status, spent, output, remaining)
        return spent_total, output_total

    def _block_if_account_restricted(self, state, edge, exc):
        if "symbol is not permitted for this account" not in str(exc).lower():
            return
        if self.state_store.block_symbol(edge.symbol, str(exc)):
            logger.critical("symbol blocked after Binance account rejection symbol=%s", edge.symbol)
        state.setdefault("blocked_symbols", []).append(edge.symbol)

    @staticmethod
    def _record_commissions(state, fill, edge):
        """Separate route-asset fees from fees debited in a third asset (BNB)."""
        totals = state.setdefault("commissions", {})
        external = state.setdefault("external_commissions", {})
        for asset, amount in fill.commissions:
            value = Decimal(str(amount))
            totals[asset] = Decimal(str(totals.get(asset, 0))) + value
            if asset not in {edge.source, edge.target}:
                external[asset] = Decimal(str(external.get(asset, 0))) + value

    def _external_commission_value(self, state, start):
        """Value BNB (or another third-asset) commissions in the start asset."""
        external = {asset: Decimal(str(amount)) for asset, amount
                    in state.get("external_commissions", {}).items() if Decimal(str(amount)) > 0}
        if not external:
            return Decimal(0), {}
        try:
            tickers = self.client.tickers()
        except Exception:
            return Decimal(0), external
        value, unvalued = Decimal(0), {}
        for asset, amount in external.items():
            if asset == start:
                value += amount
                continue
            edge = self.client.edge(asset, start, tickers)
            if edge is None:
                unvalued[asset] = amount
                continue
            value += amount / edge.price if edge.side == "buy" else amount * edge.price
        return value, unvalued

    def _place_and_resolve(self, deal_id, leg, edge, amount, state, *, policy="MARKET", price=None):
        cid = client_id(deal_id, leg)
        record = {"leg": leg, "symbol": edge.symbol, "side": edge.side,
                  "policy": policy, "order_type": "MARKET" if policy == "MARKET" else "LIMIT",
                  "time_in_force": None if policy == "MARKET" else policy,
                  "limit_price": price, "input_amount": amount,
                  "client_order_id": cid, "status": "SUBMITTING"}
        state["orders"].append(record)
        state["status"] = f"LEG_{leg}_SUBMITTING"
        self.state_store.save(deal_id, state)
        logger.warning("order submitting deal_id=%s leg=%s policy=%s symbol=%s side=%s "
                       "input=%s limit_price=%s client_order_id=%s",
                       deal_id, leg, policy, edge.symbol, edge.side, amount, price, cid)
        try:
            if policy == "MARKET":
                fill = self.client.new_market_order(edge, amount, cid)
            else:
                fill = self.client.new_limit_order(edge, amount, price, policy, cid)
        except (requests.RequestException, BinanceError) as exc:
            if isinstance(exc, BinanceError) and not exc.ambiguous:
                raise
            try:
                fill = self._resolve_client_order(edge.symbol, cid)
            except Exception as resolve_error:
                raise AmbiguousOrderError(
                    f"leg {leg} placement could not be resolved ({type(resolve_error).__name__})"
                ) from resolve_error
            if fill is None:
                raise AmbiguousOrderError(f"leg {leg} placement outcome unknown: {exc}") from exc
            logger.warning("order placement response recovered deal_id=%s leg=%s symbol=%s "
                           "client_order_id=%s order_id=%s",
                           deal_id, leg, edge.symbol, cid, fill.order_id)
        record.update(order_id=fill.order_id, status=fill.status)
        self.state_store.save(deal_id, state)
        if not fill.terminal:
            try:
                fill = self._wait_terminal(edge.symbol, fill.order_id)
            except Exception as poll_error:
                raise AmbiguousOrderError(
                    f"leg {leg} terminal state unknown ({type(poll_error).__name__})"
                ) from poll_error
        if fill is None:
            raise AmbiguousOrderError(f"order {record.get('order_id')} has no readable terminal state")
        record.update(status=fill.status, filled_volume=fill.volume, filled_cost=fill.cost,
                      commissions=dict(fill.commissions))
        self.state_store.save(deal_id, state)
        if fill.symbol != edge.symbol or fill.side != edge.side:
            raise AmbiguousOrderError("resolved order identity differs from persisted intent")
        return fill

    def _resolve_client_order(self, symbol, cid):
        for _ in range(7):
            try:
                return self.client.get_order(symbol, client_order_id=cid)
            except BinanceError as exc:
                if exc.code != -2013:
                    raise
            time.sleep(.25)
        return None

    def _wait_terminal(self, symbol, order_id):
        for _ in range(self.poll_attempts):
            fill = self.client.get_order(symbol, order_id=order_id)
            if fill.terminal:
                return fill
            time.sleep(self.poll_interval_s)
        return None

    @staticmethod
    def _update_inventory(state, edge, spent, output):
        inventory = state["inventory"]
        inventory[edge.source] = max(Decimal(str(inventory.get(edge.source, 0))) - spent, Decimal(0))
        inventory[edge.target] = Decimal(str(inventory.get(edge.target, 0))) + output

    def _await_credit(self, asset, before, expected):
        threshold = before + expected * Decimal("0.99")
        for _ in range(self.settlement_attempts):
            if self.client.balances().get(asset, Decimal(0)) >= threshold:
                return True
            time.sleep(self.settlement_interval_s)
        return False

    def _recover_to_start(self, deal_id, state, start):
        state["recovery_attempted"] = True
        positions = [(asset, Decimal(str(amount))) for asset, amount in state["inventory"].items()
                     if asset != start and Decimal(str(amount)) > 0]
        if not positions:
            return True
        logger.warning("recovery start deal_id=%s target=%s positions=%s", deal_id, start, positions)
        tickers, ok = self.client.tickers(), True
        for asset, amount in positions:
            edge = self.client.edge(asset, start, tickers)
            if edge is None:
                ok = False
                continue
            try:
                self.client.prepare_market_order(edge, amount)
            except ValueError:
                state.setdefault("residual_dust", {})[asset] = amount
                logger.info("recovery dust deal_id=%s asset=%s amount=%s", deal_id, asset, amount)
                continue
            try:
                fill = self._place_and_resolve(deal_id, f"recovery-{asset}", edge, amount, state)
                output = max((fill.volume if edge.side == "buy" else fill.cost)
                             - fill.commission(edge.target), Decimal(0))
                spent = (fill.cost if edge.side == "buy" else fill.volume) + fill.commission(edge.source)
                self._record_commissions(state, fill, edge)
                self._update_inventory(state, edge, spent, output)
                self.state_store.save(deal_id, state)
                remainder = Decimal(str(state["inventory"].get(asset, 0)))
                if remainder > 0:
                    try:
                        self.client.prepare_market_order(edge, remainder)
                    except ValueError:
                        state.setdefault("residual_dust", {})[asset] = remainder
                    else:
                        # Do not claim flattening after a partial market fill.
                        state.setdefault("recovery_errors", []).append(
                            f"{asset}: executable remainder after recovery")
                        ok = False
                logger.warning("recovery fill deal_id=%s asset=%s target=%s order_id=%s "
                               "input_spent=%s net_output=%s",
                               deal_id, asset, start, fill.order_id, spent, output)
            except AmbiguousOrderError as exc:
                state.update(ambiguous_order=True)
                state.setdefault("recovery_errors", []).append(f"{asset}: {exc}")
                return False
            except Exception as exc:
                logger.exception("recovery failed for %s", asset)
                state.setdefault("recovery_errors", []).append(f"{asset}: {exc}")
                ok = False
        return ok
