"""Bounded non-executing study of quotes excluded by the live freshness gate."""
import argparse
from dataclasses import replace
import json
from pathlib import Path
import time

from .app import Settings, bootstrap, find_best, load_env_file, make_client
from .market_stream import BookTickerStream
from .state import StateStore


def restrict_to_observation(client):
    signed = client.signed

    def checked(method, endpoint, *args, **kwargs):
        if method != 'GET' and (method, endpoint) != ('POST', '/api/v3/order/test'):
            raise RuntimeError('research prohibits exchange mutations')
        return signed(method, endpoint, *args, **kwargs)

    client.signed = checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=60, choices=range(10, 301), metavar='10..300')
    parser.add_argument('--output', default='/tmp/binarb-research')
    args = parser.parse_args()
    load_env_file()
    original = Settings.load()
    settings = replace(original, dry_run=True, state_dir=str(Path(args.output) / 'state'),
                       confirmation_candidates_total=3, confirmation_candidates_per_start=1)
    client = make_client()
    restrict_to_observation(client)
    bootstrap(client, settings)
    balances = client.balances()
    blocked = StateStore(original.state_dir).blocked_symbols()
    stream = BookTickerStream(client.pairs)
    stream.start()
    totals = {'scans': 0, 'depth_requests': 0, 'confirmed': 0, 'shadow_eligible': 0, 'rejections': {}}
    deadline = time.monotonic() + args.seconds
    try:
        while time.monotonic() < deadline and totals['depth_requests'] < 120:
            time.sleep(min(5, max(0, deadline - time.monotonic())))
            candidate, stats = find_best(client, settings, stream.snapshot(max_age_s=30), balances,
                                         blocked_symbols=blocked, research_quote_limits=(30, 30))
            totals['scans'] += 1
            totals['depth_requests'] += stats['depth_requests']
            totals['confirmed'] += stats['confirmed_candidates']
            totals['shadow_eligible'] += int(candidate is not None)
            for code, count in stats['rejection_codes'].items():
                totals['rejections'][code] = totals['rejections'].get(code, 0) + count
    finally:
        stream.stop()
    print(json.dumps(totals, sort_keys=True))


if __name__ == '__main__':
    main()
