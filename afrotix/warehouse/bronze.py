"""Bronze loader: land the saga event log in the warehouse, raw and idempotent.

Bronze is the medallion's raw zone: minimally transformed, append-only, never
rewritten. This loader reads the JSONL event log, computes a deterministic hash
per event, and inserts only events not already present — so re-running it is
safe (no duplicates), and a ``--full-refresh`` rebuilds from scratch.

What lands: the SagaEvent's own fields as typed columns, the ``payload`` as
JSON/VARIANT, and ingestion metadata (``_row_hash``, ``_source_file``,
``_batch_id``, ``_loaded_at``). Silver (dbt) cleans and reshapes from here.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from afrotix.warehouse.adapters import COLUMN_NAMES, WarehouseAdapter


@dataclass
class LoadResult:
    """Summary of one bronze load."""

    read: int
    inserted: int
    skipped: int
    total: int
    by_type: Counter = field(default_factory=Counter)


def _row_hash(event: Dict) -> str:
    """Deterministic SHA-256 over the canonical event (stable across runs)."""
    canonical = json.dumps(event, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


class BronzeLoader:
    """Reads a JSONL event log and lands it in a WarehouseAdapter's bronze table."""

    def __init__(self, adapter: WarehouseAdapter, source_path: str) -> None:
        self._adapter = adapter
        self._source = Path(source_path)

    def _read_events(self) -> List[Dict]:
        """Parse the JSONL log into a list of event dicts."""
        if not self._source.exists():
            raise FileNotFoundError(f"event log not found: {self._source}")
        with self._source.open() as f:
            return [json.loads(line) for line in f if line.strip()]

    def _to_row(self, event: Dict, batch_id: str, loaded_at: datetime) -> Dict:
        """Map a raw event to a bronze row keyed by COLUMN_NAMES."""
        return {
            "saga_id": event["saga_id"],
            "event_type": event["event_type"],
            "step": event["step"],
            "customer_id": event["customer_id"],
            "event_id": event["event_id"],
            "tier_id": event["tier_id"],
            "amount": event["amount"],
            "occurred_at": datetime.fromisoformat(event["occurred_at"]),
            "payload": json.dumps(event.get("payload", {})),
            "_row_hash": _row_hash(event),
            "_source_file": self._source.name,
            "_batch_id": batch_id,
            "_loaded_at": loaded_at,
        }

    def load(self, full_refresh: bool = False) -> LoadResult:
        """Load the event log into bronze; skip rows already present."""
        events = self._read_events()
        self._adapter.ensure_schema()
        if full_refresh:
            self._adapter.truncate()

        batch_id = f"batch_{uuid.uuid4().hex[:10]}"
        loaded_at = datetime.now(timezone.utc)
        seen = self._adapter.existing_hashes()

        new_rows: List[Dict] = []
        by_type: Counter = Counter()
        for event in events:
            by_type[event["event_type"]] += 1
            row = self._to_row(event, batch_id, loaded_at)
            if row["_row_hash"] not in seen:
                new_rows.append(row)
                seen.add(row["_row_hash"])  # guard against dupes within one file

        inserted = self._adapter.insert(new_rows)
        return LoadResult(
            read=len(events),
            inserted=inserted,
            skipped=len(events) - inserted,
            total=self._adapter.count(),
            by_type=by_type,
        )
