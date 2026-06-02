"""Domain models for AFROTIX.

Defines the core entities for the Afrobeats event-ticketing system and the
contract for the saga lifecycle events that flow into the analytics medallion.

These dataclasses are the single source of truth twice over:
  * for the catalog (dimension) data generated in Phase 1, and
  * for the bronze event schema the saga orchestrator emits in later phases.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Dict, List


class EventStatus(str, Enum):
    """Lifecycle states of a ticketed event."""

    SCHEDULED = "scheduled"
    ON_SALE = "on_sale"
    SOLD_OUT = "sold_out"
    CANCELLED = "cancelled"
    COMPLETED = "completed"


class SagaEventType(str, Enum):
    """State transitions emitted by the purchase saga.

    RESERVED/CHARGED/ISSUED are the 'complete' (happy) path. RELEASED/REFUNDED/
    VOIDED are their 'compensate' counterparts. CLOSED is the 'after' callback
    that fires once the saga finalizes, either way.
    """

    RESERVED = "reserved"   # inventory hold placed
    CHARGED = "charged"     # payment captured
    ISSUED = "issued"       # ticket issued (QR)
    RELEASED = "released"   # compensation: hold released
    REFUNDED = "refunded"   # compensation: payment refunded
    VOIDED = "voided"       # compensation: ticket voided
    CLOSED = "closed"       # after: saga finalized


class SagaStatus(str, Enum):
    """Terminal outcome of a purchase saga."""

    COMPLETED = "completed"      # every step confirmed
    COMPENSATED = "compensated"  # rolled back via compensations


@dataclass
class Artist:
    """An Afrobeats performer who headlines events."""

    artist_id: str
    name: str
    subgenre: str
    country: str
    monthly_listeners: int


@dataclass
class Venue:
    """A physical venue that hosts events."""

    venue_id: str
    name: str
    city: str
    country: str
    capacity: int


@dataclass
class TicketTier:
    """A priced inventory bucket within an event (e.g. GA, VIP).

    ``quantity_available`` is the field the Inventory service holds against and
    releases during the saga; ``quantity_total`` is the immutable allocation.
    """

    tier_id: str
    event_id: str
    name: str
    price: float
    quantity_total: int
    quantity_available: int


@dataclass
class Event:
    """A scheduled show: one artist at one venue on one date."""

    event_id: str
    name: str
    artist_id: str
    venue_id: str
    event_date: date
    doors_time: str
    status: EventStatus
    tiers: List[TicketTier] = field(default_factory=list)


@dataclass
class Customer:
    """A fan who can buy tickets."""

    customer_id: str
    name: str
    email: str
    city: str
    loyalty_tier: str
    created_at: datetime


@dataclass
class SagaEvent:
    """A single state transition in a purchase saga (the bronze contract).

    One purchase attempt (``saga_id``) emits several of these in sequence. The
    analytics plane reconstructs the full lifecycle per ``saga_id`` in silver
    and measures success vs compensation in gold (``fct_saga_outcomes``).
    """

    saga_id: str
    event_type: SagaEventType
    step: int
    customer_id: str
    event_id: str
    tier_id: str
    amount: float
    occurred_at: datetime
    payload: Dict[str, object] = field(default_factory=dict)
