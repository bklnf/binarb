"""Durable trade PnL and a retryable Telegram outbox, independent of archive retention."""
from contextlib import contextmanager
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import time


class TradeReports:
    def __init__(self, state_dir):
        self.path = Path(state_dir).parent / 'trade_reports.sqlite3'

    @contextmanager
    def connection(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path, timeout=10) as db:
            db.execute('''CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY, asset TEXT NOT NULL, status TEXT NOT NULL,
                pnl TEXT, pnl_usdt TEXT, created REAL NOT NULL, message TEXT NOT NULL,
                sent INTEGER NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                retry_at REAL NOT NULL DEFAULT 0)''')
            yield db

    def record(self, state, *, notify=True):
        if state.get('kind') == 'REBALANCE':
            return
        if state.get('status') not in {'COMPLETE', 'COMPLETE_WITH_DUST', 'RECOVERED'}:
            return
        route = state.get('route', [])
        if not route or not state.get('orders'):
            return
        # A rejected order alone is not a completed trade.
        if not any(Decimal(str(o.get('filled_volume', 0))) > 0 for o in state['orders']):
            return
        asset, pnl = route[0], state.get('realized_pnl')
        usd = state.get('realized_pnl_usdt', pnl if asset == 'USDT' else None)
        label = 'Trade completed' if state['status'].startswith('COMPLETE') else 'Trade recovered'
        text = (f"{'✅' if state['status'].startswith('COMPLETE') else '⚠️'} {label}\n"
                f"{' → '.join(route)}\n"
                f"Realized PnL: {pnl if pnl is not None else 'unknown'} {asset}\n"
                f"Status: {state['status']}\nDeal: {state['deal_id']}")
        if usd is not None and asset != 'USDT':
            text += f'\n≈ {Decimal(str(usd)):+.6f} USDT at completion'
        text += '\nAfter recorded commissions; residual dust unvalued.'
        with self.connection() as db:
            db.execute('INSERT OR IGNORE INTO trades '
                       '(id,asset,status,pnl,pnl_usdt,created,message,sent) VALUES (?,?,?,?,?,?,?,?)',
                       (state['deal_id'], asset, state['status'],
                        str(pnl) if pnl is not None else None,
                        str(usd) if usd is not None else None,
                        state.get('completed_at', state.get('created_at', time.time())),
                        text, int(not notify)))

    def import_archives(self, state_dir):
        for path in sorted((Path(state_dir) / 'archive').glob('*.json')):
            self.record(json.loads(path.read_text()), notify=False)

    def summary(self):
        totals, count, unknown, usd, usd_missing = {}, 0, 0, Decimal(0), 0
        with self.connection() as db:
            for asset, status, pnl, pnl_usdt in db.execute('SELECT asset,status,pnl,pnl_usdt FROM trades'):
                count += status.startswith('COMPLETE')
                if pnl is None:
                    unknown += 1
                else:
                    totals[asset] = totals.get(asset, Decimal(0)) + Decimal(pnl)
                if pnl_usdt is None:
                    usd_missing += 1
                else:
                    usd += Decimal(pnl_usdt)
        return {'completed_trades': count, 'realized_pnl_by_asset': {a: str(v) for a, v in totals.items()},
                'realized_pnl_usdt': str(usd), 'pnl_unknown_trades': unknown,
                'pnl_usdt_unvalued_trades': usd_missing}

    def deliver(self, send, *, limit=5, now=None):
        now = time.time() if now is None else now
        # Gateway is the sole delivery worker. Persist after successful delivery;
        # a crash after Telegram accepts may duplicate a message with the same ID.
        with self.connection() as db:
            pending = db.execute('SELECT id,message,attempts FROM trades '
                                 'WHERE sent=0 AND retry_at<=? ORDER BY created LIMIT ?',
                                 (now, limit)).fetchall()
        for deal_id, message, attempts in pending:
            try:
                accepted = send(message)
            except Exception:
                accepted = False
            with self.connection() as db:
                if accepted:
                    db.execute('UPDATE trades SET sent=1 WHERE id=?', (deal_id,))
                else:
                    delay = min(3600, 30 * 2 ** min(attempts, 7))
                    db.execute('UPDATE trades SET attempts=attempts+1,retry_at=? WHERE id=?',
                               (now + delay, deal_id))
