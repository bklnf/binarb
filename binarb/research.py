"""Bounded private market captures and credential-free, deterministic replay.

Only the explicitly constructed schema is written. No account response, order,
credential, environment, or arbitrary diagnostic dictionary is serialized.
"""
from dataclasses import asdict, replace
from decimal import Decimal
import argparse
import json
import logging
import os
from pathlib import Path
import time
import uuid

from .models import Book, Edge, Level, PairMeta
from .scanner import best_size_detailed


MAX_FILES = 200
MAX_BYTES = 32 * 1024 * 1024
MAX_FILE_BYTES = 512 * 1024
MAX_AGE = 7 * 86400
_last_capture = {}
logger = logging.getLogger(__name__)


def _result(edges, books, pairs, minimum, maximum, threshold):
    candidate, diagnostic = best_size_detailed(edges, books, pairs, minimum, maximum, threshold)
    return {"code": diagnostic['code'], "best_net_bps": diagnostic['best_net_bps'],
            "profit": candidate.profit if candidate else None,
            "external_fee_value": candidate.external_fee_value if candidate else None}


def compare(edges, books, pairs, minimum, maximum, threshold):
    legacy = tuple(replace(e, fee_asset=None, fee_conversion=Decimal(1),
                           fee_value=Decimal(0), fee_available=Decimal(0)) for e in edges)
    return {"received_asset": _result(legacy, books, pairs, minimum, maximum, threshold),
            "asset_aware": _result(edges, books, pairs, minimum, maximum, threshold)}


def capture(state_dir, edges, books, pairs, minimum, maximum, threshold, **context):
    if os.getenv('ARB_CAPTURE_ENABLED_BINANCE', 'true').lower() == 'false':
        return None
    directory = Path(state_dir).parent / 'research'
    key, now = str(directory), time.monotonic()
    if now - _last_capture.get(key, -100) < 10:
        return None
    _last_capture[key] = now
    try:
        return write_capture(directory, edges, books, pairs, minimum, maximum, threshold, **context)
    except (OSError, ValueError):
        # Research I/O must not change the live selection decision.
        logger.warning('market capture unavailable', exc_info=False)
        return None


def write_capture(directory, edges, books, pairs, minimum, maximum, threshold, *,
                  ticker_gate=None, price_change=None, research=False):
    directory = Path(directory)
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    now = time.monotonic()
    payload = {"schema": 1, "captured_at": time.time(),
               "strict_ticker_gate": ticker_gate, "price_change_bps": price_change,
               "research_sample": research,
               "edges": [asdict(e) for e in edges],
               "books": {s: asdict(b) for s, b in books.items()},
               "pairs": {s: asdict(pairs[s]) for s in books},
               "minimum": minimum, "maximum": maximum, "threshold": threshold,
               "book_ages_s": {s: now - b.observed_at if b.observed_at else None
                               for s, b in books.items()},
               "comparison": compare(edges, books, pairs, minimum, maximum, threshold)}
    encoded = json.dumps(payload, default=str, sort_keys=True).encode()
    if len(encoded) > MAX_FILE_BYTES:
        raise ValueError('capture exceeds size limit')
    for temporary in directory.glob('market-*.tmp'):
        if time.time() - temporary.stat().st_mtime > 60:
            temporary.unlink()
    files = sorted(directory.glob('market-*.json'), key=lambda p: p.stat().st_mtime)
    for path in list(files):
        if time.time() - path.stat().st_mtime > MAX_AGE:
            path.unlink()
            files.remove(path)
    total = sum(p.stat().st_size for p in files)
    while files and (len(files) >= MAX_FILES or total + len(encoded) > MAX_BYTES):
        path = files.pop(0)
        total -= path.stat().st_size
        path.unlink()
    path = directory / ('market-' + uuid.uuid4().hex + '.json')
    # Exclusive creation prevents overwriting existing files or following links.
    temporary = path.with_suffix('.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path


def replay(path):
    path = Path(path)
    if path.stat().st_size > MAX_FILE_BYTES:
        raise ValueError('capture exceeds size limit')
    data = json.loads(path.read_text())
    if data['schema'] != 1:
        raise ValueError('unsupported capture schema')
    edges = tuple(Edge(**{k: Decimal(v) if k in {
        'price', 'fee', 'fee_conversion', 'fee_value', 'fee_available'} else v
        for k, v in row.items()}) for row in data['edges'])
    books = {s: Book(tuple(Level(Decimal(x['price']), Decimal(x['quantity'])) for x in b['bids']),
                     tuple(Level(Decimal(x['price']), Decimal(x['quantity'])) for x in b['asks']),
                     b['observed_at']) for s, b in data['books'].items()}
    names = {'symbol', 'base', 'quote', 'quote_precision', 'min_notional_apply_market',
             'max_notional_apply_market', 'quote_order_qty_market_allowed'}
    pairs = {s: PairMeta(**{k: v if k in names or v is None else Decimal(v)
                            for k, v in p.items()}) for s, p in data['pairs'].items()}
    return compare(edges, books, pairs, *(Decimal(data[k]) for k in ('minimum', 'maximum', 'threshold')))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('path', help='capture file or directory; replay never loads exchange credentials')
    args = parser.parse_args()
    path = Path(args.path)
    files = sorted(path.glob('market-*.json'))[:MAX_FILES] if path.is_dir() else [path]
    results = [replay(p) for p in files]
    summary = {"captures": len(results),
               "received_asset_eligible": sum(r['received_asset']['code'] == 'ELIGIBLE' for r in results),
               "asset_aware_eligible": sum(r['asset_aware']['code'] == 'ELIGIBLE' for r in results),
               "comparisons": results}
    print(json.dumps(summary, default=str))


if __name__ == '__main__':
    main()
