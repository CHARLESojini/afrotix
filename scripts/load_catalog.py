"""Load the seeded catalog (reference data) into the warehouse bronze layer.

Events arrive via ``load_bronze``; this lands the dimensions — artists, venues,
events, customers, ticket tiers — as ``raw_*`` tables so dbt can build conformed
dimensions from the same warehouse the events live in. Target follows the
``WAREHOUSE`` env var (duckdb | snowflake).

Usage:
    python -m scripts.load_catalog
"""
from __future__ import annotations

import os

from afrotix.warehouse.adapters import get_adapter

try:  # optional: load a local .env if present
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

CATALOG = {
    "raw_artists": "data/seed/artists.json",
    "raw_venues": "data/seed/venues.json",
    "raw_customers": "data/seed/customers.json",
    "raw_events": "data/seed/events.json",
    "raw_ticket_tiers": "data/seed/ticket_tiers.json",
}


def main() -> None:
    """Land every catalog file as a bronze raw_* table."""
    target = os.getenv("WAREHOUSE", "duckdb")
    adapter = get_adapter()
    try:
        print(f"Catalog load -> {target}")
        for table, path in CATALOG.items():
            count = adapter.replace_table_from_json(table, path)
            print(f"  {table:18s} {count:6d} rows  <- {path}")
    finally:
        adapter.close()


if __name__ == "__main__":
    main()
