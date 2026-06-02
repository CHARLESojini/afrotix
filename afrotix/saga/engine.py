"""The purchase saga: orchestrates reserve -> charge -> issue, with rollback.

Analogy: a bartender taking a card. They pour the drink (reserve), run the card
(charge), then hand it over (issue). If the card declines after pouring, they
tip the drink back (release). The golden rule of a saga is the same: if a later
step fails, undo the earlier ones in reverse order.

This implements the article's LRA callbacks:
  * complete   -> confirm each step          (RESERVED, CHARGED, ISSUED)
  * compensate -> run inverses in reverse    (VOIDED, REFUNDED, RELEASED)
  * after      -> emit a terminal CLOSED event once the saga finalizes

Every transition is written to the EventLog, the contract the analytics
medallion consumes downstream.
"""
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from afrotix.models import SagaEvent, SagaEventType, SagaStatus
from afrotix.saga.eventlog import EventLog
from afrotix.services import (
    InventoryService,
    PaymentDeclined,
    PaymentsService,
    SoldOut,
    TicketingService,
)


@dataclass
class SagaResult:
    """The terminal outcome of one purchase saga."""

    saga_id: str
    status: SagaStatus
    reason: Optional[str] = None
    failed_step: Optional[int] = None
    latency_ms: float = 0.0


def _now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


class SagaEngine:
    """Coordinates the three services for a single ticket purchase."""

    def __init__(
        self,
        inventory: InventoryService,
        payments: PaymentsService,
        ticketing: TicketingService,
        log: EventLog,
    ) -> None:
        self._inventory = inventory
        self._payments = payments
        self._ticketing = ticketing
        self._log = log

    def purchase(
        self,
        *,
        customer_id: str,
        event_id: str,
        tier_id: str,
        amount: float,
        qty: int = 1,
        force_payment_decline: bool = False,
        cancel_after: bool = False,
    ) -> SagaResult:
        """Run a purchase saga and return its terminal result.

        ``force_payment_decline`` deterministically fails the charge (test hook).
        ``cancel_after`` simulates a fan cancelling a *successful* purchase,
        which triggers a full compensation flagged as a customer cancellation.
        """
        saga_id = f"saga_{uuid.uuid4().hex[:10]}"
        start = time.perf_counter()

        def emit(event_type: SagaEventType, step: int, **payload: object) -> None:
            """Write one transition to the event log."""
            self._log.emit(
                SagaEvent(
                    saga_id=saga_id,
                    event_type=event_type,
                    step=step,
                    customer_id=customer_id,
                    event_id=event_id,
                    tier_id=tier_id,
                    amount=amount,
                    occurred_at=_now(),
                    payload=payload,
                )
            )

        def close(
            status: SagaStatus,
            reason: Optional[str] = None,
            failed_step: Optional[int] = None,
        ) -> SagaResult:
            """Emit the terminal CLOSED event (the 'after' callback)."""
            latency_ms = round((time.perf_counter() - start) * 1000, 2)
            emit(
                SagaEventType.CLOSED,
                0,
                status=status.value,
                reason=reason,
                failed_step=failed_step,
                latency_ms=latency_ms,
            )
            return SagaResult(saga_id, status, reason, failed_step, latency_ms)

        # Step 1 - reserve inventory.
        try:
            hold_id = self._inventory.reserve(tier_id, qty)
            emit(SagaEventType.RESERVED, 1, hold_id=hold_id, qty=qty)
        except SoldOut:
            return close(SagaStatus.COMPENSATED, reason="sold_out", failed_step=1)

        # Step 2 - charge payment.
        try:
            payment_id = self._payments.charge(
                saga_id, amount, force_decline=force_payment_decline
            )
            emit(SagaEventType.CHARGED, 2, payment_id=payment_id)
        except PaymentDeclined:
            self._inventory.release(hold_id)
            emit(SagaEventType.RELEASED, 1, hold_id=hold_id)
            return close(SagaStatus.COMPENSATED, reason="payment_declined", failed_step=2)

        # Step 3 - issue ticket.
        try:
            ticket_id = self._ticketing.issue(saga_id, event_id, tier_id, customer_id)
            emit(SagaEventType.ISSUED, 3, ticket_id=ticket_id)
        except Exception:  # noqa: BLE001 - any issuance failure triggers rollback
            self._payments.refund(payment_id)
            emit(SagaEventType.REFUNDED, 2, payment_id=payment_id)
            self._inventory.release(hold_id)
            emit(SagaEventType.RELEASED, 1, hold_id=hold_id)
            return close(SagaStatus.COMPENSATED, reason="issue_error", failed_step=3)

        # Optional - fan cancels a successful purchase: undo everything in reverse.
        if cancel_after:
            self._ticketing.void(ticket_id)
            emit(SagaEventType.VOIDED, 3, ticket_id=ticket_id)
            self._payments.refund(payment_id)
            emit(SagaEventType.REFUNDED, 2, payment_id=payment_id)
            self._inventory.release(hold_id)
            emit(SagaEventType.RELEASED, 1, hold_id=hold_id)
            return close(SagaStatus.COMPENSATED, reason="customer_cancellation")

        return close(SagaStatus.COMPLETED)
