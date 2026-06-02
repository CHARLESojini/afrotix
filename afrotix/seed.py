"""Faker-based catalog seeder for AFROTIX (Phase 1).

Generates the reference/dimension data — artists, venues, customers, and events
with their ticket tiers — and writes it to ``data/seed/`` as normalized JSON
(one file per entity, foreign keys only — warehouse-friendly).

The transactional saga events are produced later by the orchestrator (Phase 2),
not here: this script only builds the catalog the saga operates on.

Usage:
    python -m afrotix.seed --artists 20 --venues 12 --customers 400 --events 50
"""
from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import asdict
from datetime import date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Dict, List

from faker import Faker

from afrotix.models import (
    Artist,
    Customer,
    Event,
    EventStatus,
    TicketTier,
    Venue,
)

fake = Faker()

# Curated Afrobeats flavor so the catalog reads realistically rather than as
# random noise. Names are used purely as sample catalog data.
ARTIST_POOL: List[str] = [
    "Burna Boy", "Wizkid", "Davido", "Tems", "Rema", "Asake", "Ayra Starr",
    "Fireboy DML", "Omah Lay", "CKay", "Tiwa Savage", "Mr Eazi", "Joeboy",
    "Adekunle Gold", "Olamide", "Yemi Alade", "Oxlade", "Ruger", "Lojay",
    "Victony", "Pheelz", "BNXN", "Tyla", "Libianca", "Seyi Vibez",
]
SUBGENRES: List[str] = [
    "Afrobeats", "Afro-fusion", "Afropop", "Afroswing", "Alté", "Amapiano",
]
# (name, city, country, capacity) — Boston first, since that is home turf.
VENUE_POOL: List[tuple] = [
    ("House of Blues", "Boston", "USA", 2400),
    ("MGM Music Hall at Fenway", "Boston", "USA", 5000),
    ("Roadrunner", "Boston", "USA", 3500),
    ("Big Night Live", "Boston", "USA", 2000),
    ("O2 Academy Brixton", "London", "UK", 4900),
    ("Madison Square Garden", "New York", "USA", 20000),
    ("Accor Arena", "Paris", "France", 20300),
    ("Eko Convention Centre", "Lagos", "Nigeria", 4000),
    ("Accra Sports Stadium", "Accra", "Ghana", 40000),
    ("Scotiabank Arena", "Toronto", "Canada", 19800),
    ("State Farm Arena", "Atlanta", "USA", 16600),
    ("AccorHotels Arena", "Amsterdam", "Netherlands", 17000),
]
# (tier name, price multiplier, capacity weight)
TIER_TEMPLATES: List[tuple] = [
    ("Early Bird", 0.6, 0.15),
    ("General Admission", 1.0, 0.45),
    ("Balcony", 1.3, 0.20),
    ("VIP", 2.2, 0.12),
    ("Pit", 1.8, 0.08),
]
LOYALTY_TIERS: List[str] = ["bronze", "silver", "gold", "platinum"]
EVENT_STATUS_WEIGHTS: Dict[EventStatus, float] = {
    EventStatus.ON_SALE: 0.6,
    EventStatus.SCHEDULED: 0.2,
    EventStatus.SOLD_OUT: 0.12,
    EventStatus.COMPLETED: 0.05,
    EventStatus.CANCELLED: 0.03,
}


def _new_id(prefix: str) -> str:
    """Return a short, prefixed unique id (e.g. ``art_1a2b3c4d``)."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def generate_artists(count: int) -> List[Artist]:
    """Generate ``count`` artists, drawing from the curated pool first."""
    names = list(ARTIST_POOL)
    random.shuffle(names)
    while len(names) < count:
        names.append(f"{fake.first_name()} {fake.last_name()}")
    return [
        Artist(
            artist_id=_new_id("art"),
            name=names[i],
            subgenre=random.choice(SUBGENRES),
            country=random.choice(["Nigeria", "Ghana", "South Africa", "UK", "USA"]),
            monthly_listeners=random.randint(150_000, 45_000_000),
        )
        for i in range(count)
    ]


def generate_venues(count: int) -> List[Venue]:
    """Generate ``count`` venues, drawing from the curated pool first."""
    pool = list(VENUE_POOL)
    random.shuffle(pool)
    venues: List[Venue] = []
    for i in range(count):
        if i < len(pool):
            name, city, country, capacity = pool[i]
        else:
            name = f"{fake.city()} Arena"
            city, country, capacity = fake.city(), fake.country(), random.randint(1500, 18000)
        venues.append(
            Venue(
                venue_id=_new_id("ven"),
                name=name,
                city=city,
                country=country,
                capacity=capacity,
            )
        )
    return venues


def generate_customers(count: int) -> List[Customer]:
    """Generate ``count`` fans with plausible signup history."""
    customers: List[Customer] = []
    for _ in range(count):
        first, last = fake.first_name(), fake.last_name()
        customers.append(
            Customer(
                customer_id=_new_id("cus"),
                name=f"{first} {last}",
                email=f"{first}.{last}@{fake.free_email_domain()}".lower(),
                city=fake.city(),
                loyalty_tier=random.choices(LOYALTY_TIERS, weights=[0.5, 0.3, 0.15, 0.05])[0],
                created_at=fake.date_time_between(start_date="-2y", end_date="now"),
            )
        )
    return customers


def _build_tiers(event_id: str, capacity: int) -> List[TicketTier]:
    """Build 2-4 ticket tiers for an event, splitting venue capacity by weight."""
    chosen = random.sample(TIER_TEMPLATES, k=random.randint(2, 4))
    base_price = round(random.uniform(45, 120), 2)
    for_sale = int(capacity * random.uniform(0.7, 1.0))
    weight_sum = sum(w for _, _, w in chosen)
    tiers: List[TicketTier] = []
    for name, price_mult, weight in chosen:
        qty = max(20, int(for_sale * weight / weight_sum))
        tiers.append(
            TicketTier(
                tier_id=_new_id("tier"),
                event_id=event_id,
                name=name,
                price=round(base_price * price_mult, 2),
                quantity_total=qty,
                quantity_available=qty,
            )
        )
    return tiers


def generate_events(
    count: int, artists: List[Artist], venues: List[Venue]
) -> tuple:
    """Generate ``count`` events plus their flattened ticket tiers.

    Returns a ``(events, tiers)`` pair so each can be written as its own
    normalized dimension table.
    """
    statuses = list(EVENT_STATUS_WEIGHTS.keys())
    weights = list(EVENT_STATUS_WEIGHTS.values())
    events: List[Event] = []
    all_tiers: List[TicketTier] = []
    for _ in range(count):
        artist = random.choice(artists)
        venue = random.choice(venues)
        event_id = _new_id("evt")
        event_date = date.today() + timedelta(days=random.randint(14, 210))
        tiers = _build_tiers(event_id, venue.capacity)
        all_tiers.extend(tiers)
        events.append(
            Event(
                event_id=event_id,
                name=f"{artist.name} Live in {venue.city}",
                artist_id=artist.artist_id,
                venue_id=venue.venue_id,
                event_date=event_date,
                doors_time=random.choice(["18:00", "18:30", "19:00", "19:30", "20:00"]),
                status=random.choices(statuses, weights=weights)[0],
            )
        )
    return events, all_tiers


def _json_default(value: object) -> str:
    """Serialize dates and enums that the json module cannot encode natively."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Unserializable type: {type(value)!r}")


def _write(records: List[object], path: Path, drop: tuple = ()) -> None:
    """Write a list of dataclass records to ``path`` as a JSON array.

    ``drop`` names fields to omit (used to keep the nested ``tiers`` list out
    of the normalized events file).
    """
    rows = []
    for record in records:
        row = asdict(record)
        for key in drop:
            row.pop(key, None)
        rows.append(row)
    path.write_text(json.dumps(rows, default=_json_default, indent=2))


def main() -> None:
    """Parse args, generate the catalog, and write normalized seed files."""
    parser = argparse.ArgumentParser(description="Seed the AFROTIX catalog.")
    parser.add_argument("--artists", type=int, default=20)
    parser.add_argument("--venues", type=int, default=12)
    parser.add_argument("--customers", type=int, default=400)
    parser.add_argument("--events", type=int, default=50)
    parser.add_argument("--out", type=str, default="data/seed")
    parser.add_argument("--seed", type=int, default=42, help="RNG seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)
    Faker.seed(args.seed)

    artists = generate_artists(args.artists)
    venues = generate_venues(args.venues)
    customers = generate_customers(args.customers)
    events, tiers = generate_events(args.events, artists, venues)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write(artists, out_dir / "artists.json")
    _write(venues, out_dir / "venues.json")
    _write(customers, out_dir / "customers.json")
    _write(events, out_dir / "events.json", drop=("tiers",))
    _write(tiers, out_dir / "ticket_tiers.json")

    print(
        f"Seeded catalog into {out_dir}/:\n"
        f"  artists={len(artists)}  venues={len(venues)}  "
        f"customers={len(customers)}  events={len(events)}  tiers={len(tiers)}"
    )


if __name__ == "__main__":
    main()
