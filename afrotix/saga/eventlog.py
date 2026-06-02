"""Append-only event log for saga lifecycle events (the bronze contract).

Each SagaEvent is serialized to one JSON object per line (JSONL), so the file
is an immutable, append-only record of every state transition — exactly the
shape Phase 3 lands in the Snowflake bronze layer. Swapping this sink for a
Snowflake or Kafka writer later means changing only this module.
"""
from __future__ import annotations

import json
from dataclasses import asdict
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import List

from afrotix.models import SagaEvent


def _json_default(value: object) -> str:
    """Serialize datetimes and enums the json module cannot encode natively."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unserializable type: {type(value)!r}")


class EventLog:
    """Appends SagaEvents to a JSONL file and keeps them in memory.

    The in-memory ``records`` list is a convenience for tests and run summaries;
    the file on disk is the durable artifact the analytics plane consumes.
    """

    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.records: List[SagaEvent] = []

    def emit(self, event: SagaEvent) -> None:
        """Persist one event as a JSON line and retain it in memory."""
        self.records.append(event)
        with self.path.open("a") as f:
            f.write(json.dumps(asdict(event), default=_json_default) + "\n")
