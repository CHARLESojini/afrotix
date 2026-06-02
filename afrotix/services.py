"""Operational services for the AFROTIX purchase saga.

Three independent services, each owning its OWN database (a separate SQLite
file locally, or a Postgres URL in deployment), mirroring a microservices
boundary:

  * Inventory  - holds and releases ticket stock
  * Payments   - charges and refunds
  * Ticketing  - issues and voids tickets

Each exposes a forward action and its compensating inverse, which is exactly
what the saga engine needs. In this phase the services run in-process (a
modular monolith); Phase 7 splits them into separate containers. Because each
keeps its own engine and schema, that split is mechanical, not a rewrite.
"""
from __future__ import annotations

import random
import uuid
from pathlib import Path
from typing import Dict, List

from sqlalchemy import Integer, String, create_engine
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    sessionmaker,
)


class SoldOut(Exception):
    """Raised when a tier lacks enough available stock to satisfy a hold."""


class PaymentDeclined(Exception):
    """Raised when a charge is rejected by the payment provider."""


def _engine(db_url: str) -> Engine:
    """Create an engine, adding SQLite-specific threading args when needed.

    For a SQLite file URL, the parent directory is created if missing so the
    service works from a clean checkout without a manual mkdir.
    """
    url = make_url(db_url)
    connect_args = {}
    if url.drivername.startswith("sqlite"):
        connect_args = {"check_same_thread": False}
        if url.database and url.database != ":memory:":
            Path(url.database).parent.mkdir(parents=True, exist_ok=True)
    return create_engine(db_url, connect_args=connect_args)


# ============================ Inventory =============================
class _InvBase(DeclarativeBase):
    """Declarative base private to the Inventory database."""


class TierStock(_InvBase):
    """Available stock for one ticket tier."""

    __tablename__ = "tier_stock"
    tier_id: Mapped[str] = mapped_column(String, primary_key=True)
    event_id: Mapped[str] = mapped_column(String)
    quantity_available: Mapped[int] = mapped_column(Integer)


class Hold(_InvBase):
    """A reservation placed against a tier during a saga."""

    __tablename__ = "holds"
    hold_id: Mapped[str] = mapped_column(String, primary_key=True)
    tier_id: Mapped[str] = mapped_column(String)
    qty: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="held")


class InventoryService:
    """Owns ticket stock; reserves (holds) and releases it."""

    def __init__(self, db_url: str) -> None:
        self._engine = _engine(db_url)
        _InvBase.metadata.create_all(self._engine)
        self._Session = sessionmaker(self._engine)

    def load_tiers(self, tiers: List[Dict]) -> None:
        """Seed stock levels from catalog ticket_tier records (idempotent)."""
        with self._Session.begin() as s:
            for t in tiers:
                if s.get(TierStock, t["tier_id"]) is None:
                    s.add(
                        TierStock(
                            tier_id=t["tier_id"],
                            event_id=t["event_id"],
                            quantity_available=t["quantity_available"],
                        )
                    )

    def reserve(self, tier_id: str, qty: int) -> str:
        """Hold ``qty`` units of ``tier_id``; raise SoldOut if insufficient."""
        with self._Session.begin() as s:
            stock = s.get(TierStock, tier_id)
            if stock is None or stock.quantity_available < qty:
                raise SoldOut(f"tier {tier_id} cannot satisfy qty={qty}")
            stock.quantity_available -= qty
            hold_id = f"hold_{uuid.uuid4().hex[:8]}"
            s.add(Hold(hold_id=hold_id, tier_id=tier_id, qty=qty))
            return hold_id

    def release(self, hold_id: str) -> None:
        """Compensation: return a hold's units to stock (idempotent)."""
        with self._Session.begin() as s:
            hold = s.get(Hold, hold_id)
            if hold is None or hold.status == "released":
                return
            stock = s.get(TierStock, hold.tier_id)
            if stock is not None:
                stock.quantity_available += hold.qty
            hold.status = "released"

    def available(self, tier_id: str) -> int:
        """Return current available units for a tier (read helper for tests)."""
        with self._Session() as s:
            stock = s.get(TierStock, tier_id)
            return stock.quantity_available if stock else 0


# ============================ Payments ==============================
class _PayBase(DeclarativeBase):
    """Declarative base private to the Payments database."""


class Payment(_PayBase):
    """A charge attempt tied to a saga."""

    __tablename__ = "payments"
    payment_id: Mapped[str] = mapped_column(String, primary_key=True)
    saga_id: Mapped[str] = mapped_column(String)
    amount_cents: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="captured")


class PaymentsService:
    """Charges fans and refunds them on compensation."""

    def __init__(self, db_url: str, failure_rate: float = 0.0) -> None:
        self._engine = _engine(db_url)
        _PayBase.metadata.create_all(self._engine)
        self._Session = sessionmaker(self._engine)
        self.failure_rate = failure_rate

    def charge(self, saga_id: str, amount: float, force_decline: bool = False) -> str:
        """Capture ``amount``; raise PaymentDeclined on a (simulated) failure.

        ``force_decline`` makes the outcome deterministic for tests; otherwise
        a charge declines with probability ``failure_rate``.
        """
        if force_decline or random.random() < self.failure_rate:
            raise PaymentDeclined(f"charge declined for saga {saga_id}")
        with self._Session.begin() as s:
            payment_id = f"pay_{uuid.uuid4().hex[:8]}"
            s.add(
                Payment(
                    payment_id=payment_id,
                    saga_id=saga_id,
                    amount_cents=round(amount * 100),
                )
            )
            return payment_id

    def refund(self, payment_id: str) -> None:
        """Compensation: refund a captured payment (idempotent)."""
        with self._Session.begin() as s:
            payment = s.get(Payment, payment_id)
            if payment is not None and payment.status != "refunded":
                payment.status = "refunded"


# ============================ Ticketing =============================
class _TixBase(DeclarativeBase):
    """Declarative base private to the Ticketing database."""


class Ticket(_TixBase):
    """An issued ticket with a QR payload."""

    __tablename__ = "tickets"
    ticket_id: Mapped[str] = mapped_column(String, primary_key=True)
    saga_id: Mapped[str] = mapped_column(String)
    event_id: Mapped[str] = mapped_column(String)
    tier_id: Mapped[str] = mapped_column(String)
    customer_id: Mapped[str] = mapped_column(String)
    qr_code: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="issued")


class TicketingService:
    """Issues tickets and voids them on compensation."""

    def __init__(self, db_url: str) -> None:
        self._engine = _engine(db_url)
        _TixBase.metadata.create_all(self._engine)
        self._Session = sessionmaker(self._engine)

    def issue(self, saga_id: str, event_id: str, tier_id: str, customer_id: str) -> str:
        """Issue a ticket and return its id."""
        with self._Session.begin() as s:
            ticket_id = f"tix_{uuid.uuid4().hex[:8]}"
            s.add(
                Ticket(
                    ticket_id=ticket_id,
                    saga_id=saga_id,
                    event_id=event_id,
                    tier_id=tier_id,
                    customer_id=customer_id,
                    qr_code=f"AFRO-{uuid.uuid4().hex[:12].upper()}",
                )
            )
            return ticket_id

    def void(self, ticket_id: str) -> None:
        """Compensation: void an issued ticket (idempotent)."""
        with self._Session.begin() as s:
            ticket = s.get(Ticket, ticket_id)
            if ticket is not None and ticket.status != "void":
                ticket.status = "void"
