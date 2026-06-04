"""Tests for the bronze loader against a temporary DuckDB.

Covers the load itself, idempotency (re-running adds nothing), full refresh, and
that landed rows are queryable with their types intact.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from afrotix.warehouse.adapters import FQ_TABLE, DuckDBAdapter
from afrotix.warehouse.bronze import BronzeLoader


def _write_log(path: Path, n: int) -> None:
    """Write ``n`` synthetic saga events as JSONL."""
    with path.open("w") as f:
        for i in range(n):
            event = {
                "saga_id": f"saga_{i}",
                "event_type": "reserved",
                "step": 1,
                "customer_id": f"cus_{i}",
                "event_id": "evt_1",
                "tier_id": "tier_1",
                "amount": 80.0,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "payload": {"hold_id": f"hold_{i}", "qty": 2},
            }
            f.write(json.dumps(event) + "\n")


def _loader(tmp_path: Path, n: int) -> BronzeLoader:
    """Build a loader over a fresh DuckDB and an n-line event log."""
    log = tmp_path / "events.jsonl"
    _write_log(log, n)
    adapter = DuckDBAdapter(str(tmp_path / "wh.duckdb"))
    return BronzeLoader(adapter, str(log))


def test_load_lands_all_rows(tmp_path: Path) -> None:
    """A first load inserts every event."""
    result = _loader(tmp_path, 5).load()
    assert result.read == 5
    assert result.inserted == 5
    assert result.total == 5


def test_reload_is_idempotent(tmp_path: Path) -> None:
    """Re-running the same load inserts nothing new."""
    loader = _loader(tmp_path, 5)
    loader.load()
    second = loader.load()
    assert second.inserted == 0
    assert second.skipped == 5
    assert second.total == 5


def test_full_refresh_rebuilds(tmp_path: Path) -> None:
    """A full refresh clears the table and reloads from scratch."""
    loader = _loader(tmp_path, 5)
    loader.load()
    refreshed = loader.load(full_refresh=True)
    assert refreshed.inserted == 5
    assert refreshed.total == 5


def test_payload_is_queryable_json(tmp_path: Path) -> None:
    """Landed payload is real JSON the warehouse can extract from."""
    loader = _loader(tmp_path, 3)
    loader.load()
    con = loader._adapter._con  # type: ignore[attr-defined]
    qty = con.execute(
        f"SELECT payload->>'qty' FROM {FQ_TABLE} LIMIT 1"
    ).fetchone()[0]
    assert str(qty) == "2"
