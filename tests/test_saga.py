"""Tests for the purchase saga: every compensation path must leave stock whole.

The core invariant: whenever a saga ends COMPENSATED, inventory must return to
exactly where it started — no stock leaked, no phantom holds.
"""
from __future__ import annotations

from pathlib import Path
from typing import Tuple

import pytest

from afrotix.models import SagaEventType, SagaStatus
from afrotix.saga.engine import SagaEngine
from afrotix.saga.eventlog import EventLog
from afrotix.services import InventoryService, PaymentsService, TicketingService

TIER_ID = "tier_demo"
EVENT_ID = "evt_demo"
CUSTOMER_ID = "cus_demo"
START_STOCK = 100


def _build(tmp_path: Path) -> Tuple[SagaEngine, InventoryService, EventLog]:
    """Build an engine backed by three isolated SQLite databases."""
    inventory = InventoryService(f"sqlite:///{tmp_path}/inv.db")
    payments = PaymentsService(f"sqlite:///{tmp_path}/pay.db")
    ticketing = TicketingService(f"sqlite:///{tmp_path}/tix.db")
    inventory.load_tiers(
        [{"tier_id": TIER_ID, "event_id": EVENT_ID, "quantity_available": START_STOCK}]
    )
    log = EventLog(f"{tmp_path}/events.jsonl")
    engine = SagaEngine(inventory, payments, ticketing, log)
    return engine, inventory, log


def _types(log: EventLog) -> list:
    """Return the ordered list of emitted event types."""
    return [e.event_type for e in log.records]


def test_happy_path_completes_and_decrements_stock(tmp_path: Path) -> None:
    """A clean purchase confirms all three steps and consumes stock."""
    engine, inventory, log = _build(tmp_path)
    result = engine.purchase(
        customer_id=CUSTOMER_ID, event_id=EVENT_ID, tier_id=TIER_ID, amount=80.0, qty=2
    )
    assert result.status == SagaStatus.COMPLETED
    assert _types(log) == [
        SagaEventType.RESERVED,
        SagaEventType.CHARGED,
        SagaEventType.ISSUED,
        SagaEventType.CLOSED,
    ]
    assert inventory.available(TIER_ID) == START_STOCK - 2


def test_payment_decline_releases_hold(tmp_path: Path) -> None:
    """A declined charge rolls back the reservation and leaves stock whole."""
    engine, inventory, log = _build(tmp_path)
    result = engine.purchase(
        customer_id=CUSTOMER_ID,
        event_id=EVENT_ID,
        tier_id=TIER_ID,
        amount=80.0,
        qty=2,
        force_payment_decline=True,
    )
    assert result.status == SagaStatus.COMPENSATED
    assert result.reason == "payment_declined"
    assert result.failed_step == 2
    assert _types(log) == [
        SagaEventType.RESERVED,
        SagaEventType.RELEASED,
        SagaEventType.CLOSED,
    ]
    assert inventory.available(TIER_ID) == START_STOCK


def test_cancel_after_issue_unwinds_everything(tmp_path: Path) -> None:
    """A post-purchase cancellation voids, refunds, and releases in reverse."""
    engine, inventory, log = _build(tmp_path)
    result = engine.purchase(
        customer_id=CUSTOMER_ID,
        event_id=EVENT_ID,
        tier_id=TIER_ID,
        amount=80.0,
        qty=3,
        cancel_after=True,
    )
    assert result.status == SagaStatus.COMPENSATED
    assert result.reason == "customer_cancellation"
    assert _types(log) == [
        SagaEventType.RESERVED,
        SagaEventType.CHARGED,
        SagaEventType.ISSUED,
        SagaEventType.VOIDED,
        SagaEventType.REFUNDED,
        SagaEventType.RELEASED,
        SagaEventType.CLOSED,
    ]
    assert inventory.available(TIER_ID) == START_STOCK


def test_sold_out_never_reserves(tmp_path: Path) -> None:
    """Requesting more than available fails before any state changes."""
    engine, inventory, log = _build(tmp_path)
    result = engine.purchase(
        customer_id=CUSTOMER_ID,
        event_id=EVENT_ID,
        tier_id=TIER_ID,
        amount=80.0,
        qty=START_STOCK + 1,
    )
    assert result.status == SagaStatus.COMPENSATED
    assert result.reason == "sold_out"
    assert _types(log) == [SagaEventType.CLOSED]
    assert inventory.available(TIER_ID) == START_STOCK
