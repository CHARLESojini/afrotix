"""FastAPI orchestrator exposing the purchase saga over HTTP.

POST /purchase runs one saga and returns its terminal outcome. Service
databases and the event-log path come from the environment, so the same app
runs against local SQLite or Postgres without code changes.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

try:  # optional convenience: load a local .env if present
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

from afrotix.saga.engine import SagaEngine
from afrotix.saga.eventlog import EventLog
from afrotix.services import InventoryService, PaymentsService, TicketingService

DB_DIR = os.getenv("AFROTIX_DB_DIR", "data/db")
EVENT_LOG = os.getenv("AFROTIX_EVENT_LOG", "data/events/saga_events.jsonl")
SEED_TIERS = os.getenv("AFROTIX_TIERS", "data/seed/ticket_tiers.json")
PAYMENT_FAILURE_RATE = float(os.getenv("PAYMENT_FAILURE_RATE", "0.0"))

app = FastAPI(title="AFROTIX Orchestrator", version="0.2.0")

_inventory = InventoryService(os.getenv("INVENTORY_DB_URL", f"sqlite:///{DB_DIR}/inv.db"))
_payments = PaymentsService(
    os.getenv("PAYMENTS_DB_URL", f"sqlite:///{DB_DIR}/pay.db"),
    failure_rate=PAYMENT_FAILURE_RATE,
)
_ticketing = TicketingService(os.getenv("TICKETING_DB_URL", f"sqlite:///{DB_DIR}/tix.db"))

# Load stock from the catalog if present, so the service can hold against it.
_tier_file = Path(SEED_TIERS)
if _tier_file.exists():
    _inventory.load_tiers(json.loads(_tier_file.read_text()))

_engine = SagaEngine(_inventory, _payments, _ticketing, EventLog(EVENT_LOG))


class PurchaseRequest(BaseModel):
    """Inbound purchase command."""

    customer_id: str
    event_id: str
    tier_id: str
    amount: float
    qty: int = 1
    force_payment_decline: bool = False
    cancel_after: bool = False


class PurchaseResponse(BaseModel):
    """Terminal outcome of a saga."""

    saga_id: str
    status: str
    reason: Optional[str] = None
    failed_step: Optional[int] = None
    latency_ms: float


@app.get("/health")
def health() -> dict:
    """Liveness probe."""
    return {"status": "ok"}


@app.post("/purchase", response_model=PurchaseResponse)
def purchase(req: PurchaseRequest) -> PurchaseResponse:
    """Run a purchase saga and return its outcome."""
    result = _engine.purchase(
        customer_id=req.customer_id,
        event_id=req.event_id,
        tier_id=req.tier_id,
        amount=req.amount,
        qty=req.qty,
        force_payment_decline=req.force_payment_decline,
        cancel_after=req.cancel_after,
    )
    return PurchaseResponse(
        saga_id=result.saga_id,
        status=result.status.value,
        reason=result.reason,
        failed_step=result.failed_step,
        latency_ms=result.latency_ms,
    )
