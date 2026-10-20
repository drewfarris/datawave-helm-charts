#!/usr/bin/env python3
"""Reject non-driver Accumulo installations before changing deployment resources."""
import argparse
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from deploy import get, run


def guard(namespace):
    releases = json.loads(run('helm', 'list', '-n', namespace, '--all', '-o', 'json'))
    if any(r['name'] == 'dwv' for r in releases):
        values = json.loads(run('helm', 'get', 'values', 'dwv', '-n', namespace, '-o', 'json')) or {}
        if not values.get('dwv-accumulo', {}).get('hybridStorage', {}).get('enabled'):
            raise RuntimeError('Existing dwv release was not created by fresh hybrid bootstrap. Fresh instances only; no migration is provided.')
    elif get('deployment', 'accumulo-manager', namespace) or get('configmap', 'accumulo-config', namespace):
        raise RuntimeError('Existing Accumulo resources found without a driver-owned release. Fresh instances only.')


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespace', required=True)
    try:
        guard(p.parse_args().namespace)
    except RuntimeError as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
