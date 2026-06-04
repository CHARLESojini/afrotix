"""Load the saga event log into the warehouse bronze layer.

The target is chosen by the ``WAREHOUSE`` env var (``duckdb`` by default,
``snowflake`` to flip). Idempotent by default; ``--full-refresh`` rebuilds.

Usage:
    python -m scripts.load_bronze
    WAREHOUSE=snowflake python -m scripts.load_bronze --full-refresh
"""
from __future__ import annotations

import argparse
import os

from afrotix.warehouse.adapters import get_adapter
from afrotix.warehouse.bronze import BronzeLoader

try:  # optional: load a local .env if present
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def main() -> None:
    """Parse args, run the load, and print a summary."""
    parser = argparse.ArgumentParser(description="Load saga events into bronze.")
    parser.add_argument("--source", default="data/events/saga_events.jsonl")
    parser.add_argument("--full-refresh", action="store_true")
    args = parser.parse_args()

    target = os.getenv("WAREHOUSE", "duckdb")
    adapter = get_adapter()
    try:
        result = BronzeLoader(adapter, args.source).load(full_refresh=args.full_refresh)
    finally:
        adapter.close()

    print(f"Bronze load -> {target} ({'full refresh' if args.full_refresh else 'incremental'})")
    print(f"  read={result.read}  inserted={result.inserted}  "
          f"skipped={result.skipped}  total_in_bronze={result.total}")
    for event_type, count in sorted(result.by_type.items(), key=lambda kv: -kv[1]):
        print(f"    {event_type:10s} {count:6d}")


if __name__ == "__main__":
    main()
