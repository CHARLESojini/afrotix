"""Drive many purchases through the saga to generate a realistic event log.

Loads the seeded catalog, then fires ``--runs`` purchases with random customers,
events, tiers, and quantities. A configurable share of charges decline and a
share of successful purchases get cancelled, so the resulting JSONL contains a
realistic mix of completed and compensated sagas — the raw material the bronze
layer ingests in Phase 3.

Usage:
    python -m scripts.simulate --runs 2000 --payment-failure-rate 0.12 --cancel-rate 0.05
"""
from __future__ import annotations

import argparse
import json
import random
from collections import Counter
from pathlib import Path
from typing import Dict, List

from afrotix.models import SagaEventType, SagaStatus
from afrotix.saga.engine import SagaEngine
from afrotix.saga.eventlog import EventLog
from afrotix.services import InventoryService, PaymentsService, TicketingService

SEED_DIR = Path("data/seed")
DB_DIR = Path("data/db")
EVENT_LOG = Path("data/events/saga_events.jsonl")


def _load(name: str) -> List[Dict]:
    """Load one seeded catalog file as a list of records."""
    return json.loads((SEED_DIR / name).read_text())


def _reset(paths: List[Path]) -> None:
    """Delete prior run artifacts so each simulation starts clean."""
    for p in paths:
        p.unlink(missing_ok=True)


def main() -> None:
    """Parse args, build the services, and run the simulation loop."""
    parser = argparse.ArgumentParser(description="Simulate AFROTIX purchases.")
    parser.add_argument("--runs", type=int, default=2000)
    parser.add_argument("--payment-failure-rate", type=float, default=0.12)
    parser.add_argument("--cancel-rate", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keep", action="store_true", help="append to existing data")
    args = parser.parse_args()

    random.seed(args.seed)

    if not args.keep:
        DB_DIR.mkdir(parents=True, exist_ok=True)
        _reset([DB_DIR / "inv.db", DB_DIR / "pay.db", DB_DIR / "tix.db", EVENT_LOG])

    customers = _load("customers.json")
    tiers = _load("ticket_tiers.json")

    inventory = InventoryService(f"sqlite:///{DB_DIR}/inv.db")
    payments = PaymentsService(
        f"sqlite:///{DB_DIR}/pay.db", failure_rate=args.payment_failure_rate
    )
    ticketing = TicketingService(f"sqlite:///{DB_DIR}/tix.db")
    inventory.load_tiers(tiers)

    log = EventLog(str(EVENT_LOG))
    engine = SagaEngine(inventory, payments, ticketing, log)

    outcomes: Counter = Counter()
    for _ in range(args.runs):
        customer = random.choice(customers)
        tier = random.choice(tiers)
        qty = random.randint(1, 4)
        amount = round(tier["price"] * qty, 2)
        # A fan only cancels a purchase that actually went through.
        cancel = random.random() < args.cancel_rate
        result = engine.purchase(
            customer_id=customer["customer_id"],
            event_id=tier["event_id"],
            tier_id=tier["tier_id"],
            amount=amount,
            qty=qty,
            cancel_after=cancel,
        )
        key = result.status.value if result.reason is None else f"{result.status.value}:{result.reason}"
        outcomes[key] += 1

    total_events = len(log.records)
    print(f"Ran {args.runs} sagas -> {total_events} events at {EVENT_LOG}")
    for key, count in sorted(outcomes.items(), key=lambda kv: -kv[1]):
        print(f"  {key:32s} {count:5d}  ({count / args.runs:5.1%})")


if __name__ == "__main__":
    main()
