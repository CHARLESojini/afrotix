# AFROTIX — Project Walkthrough (Phases 0–2)

A complete recon of what the project is, how it was built, and what every file
does. Read top to bottom once; after that it's a reference.

---

## 1. The big picture

AFROTIX is an Afrobeats event-ticketing system with **two planes**:

- **Transactional plane (the saga):** buying a ticket is a distributed
  transaction across three isolated services — Inventory, Payments, Ticketing.
  Because no single database spans all three, we coordinate them with a *saga*:
  a sequence of steps, each with a compensating "undo."
- **Analytics plane (the medallion):** every saga outcome is emitted as an
  event into an append-only log. Later phases land that log in Snowflake
  (bronze) and model it with dbt (silver/gold) so we can analyze not just what
  sold, but where the transaction machinery breaks and what it costs.

**Analogy for the saga:** booking a holiday — flight, then hotel, then car. You
can't lock all three vendors at once, so you book in sequence; if the hotel is
full after you've paid for the flight, you cancel (compensate) the flight.

### The spine: the lifecycle of one purchase

```
reserve (Inventory)  ->  charge (Payments)  ->  issue (Ticketing)  ->  CLOSED: completed
      |                       |                      |
   on fail               on decline             on error
   (sold_out)         release hold           refund + release
                      CLOSED: compensated    CLOSED: compensated
```

The golden rule: **if a later step fails, undo the earlier steps in reverse
order.** Forward is reserve → charge → issue; undo is void → refund → release.
Every transition (RESERVED, CHARGED, ISSUED, RELEASED, REFUNDED, VOIDED) and the
terminal CLOSED event get written to the event log.

This maps directly onto the article's MicroTX **LRA** callbacks:
- `complete`  → confirm each step
- `compensate`→ run the inverses in reverse
- `after`     → emit the terminal CLOSED event once the saga finalizes

---

## 2. How it was built, phase by phase

- **Phase 0 — Scaffold & architecture.** Repo skeleton, the architecture
  diagram, the learning-checkpoints habit, and the config files.
- **Phase 1 — Data model & seed.** The domain entities (and the bronze event
  contract) plus a Faker generator that produces a realistic catalog.
- **Phase 2 — The saga.** Three isolated services, the orchestration engine,
  the event log, a simulation to generate data at volume, a test suite proving
  the rollback logic, a FastAPI front door, and container files.

---

## 3. Every file, explained

### The package — `afrotix/`

**`afrotix/__init__.py`**
Marks the directory as an importable Python package and pins `__version__`.
Tiny, but it's what lets `from afrotix.models import ...` work anywhere.

**`afrotix/models.py`** — the single source of truth
Plain dataclasses and enums, no database logic. Two jobs:
1. Define the catalog entities: `Artist`, `Venue`, `TicketTier`, `Event`,
   `Customer`.
2. Define the **bronze contract**: `SagaEvent` — the exact shape of every event
   the saga emits — plus the enums `EventStatus`, `SagaEventType` (reserved,
   charged, issued, released, refunded, voided, closed), and `SagaStatus`
   (completed, compensated).
Defining `SagaEvent` here, before anything emits it, locks the contract between
the transactional and analytics planes so both sides agree on the schema.

**`afrotix/seed.py`** — the catalog generator (Phase 1)
A Faker-based script that writes the *reference/dimension* data. Key parts:
- Curated pools (`ARTIST_POOL`, `VENUE_POOL`, `TIER_TEMPLATES`) so the catalog
  reads like a real listing — real Afrobeats names, Boston venues first.
- `generate_artists / venues / customers / events` build the entities;
  `_build_tiers` splits a venue's capacity across 2–4 priced tiers.
- `_json_default` serializes dates/enums; `_write` emits one normalized JSON
  file per entity (foreign keys only, no nesting — warehouse-friendly).
- `main()` exposes `--artists/--venues/--customers/--events/--seed`, and seeds
  the RNG so runs are reproducible.
Output: `data/seed/{artists,venues,customers,events,ticket_tiers}.json`.
It deliberately does **not** generate saga events — those are produced by the
saga itself, keeping reference data and transactional facts cleanly separated.

**`afrotix/services.py`** — the three operational services (Phase 2)
Each service owns its **own database** (a separate SQLite file locally, or a
Postgres URL in deployment), mirroring a microservices boundary. Contents:
- Exceptions `SoldOut` and `PaymentDeclined` — the signals the saga reacts to.
- `_engine(db_url)` — builds the SQLAlchemy engine and, for SQLite, creates the
  file's parent directory so it works from a clean checkout.
- **Inventory** (`TierStock`, `Hold`): `load_tiers` seeds stock; `reserve`
  holds units (raises `SoldOut` if short); `release` returns them; `available`
  is a read helper for tests.
- **Payments** (`Payment`): `charge` captures money and raises
  `PaymentDeclined` either deterministically (`force_decline`, for tests) or
  randomly (`failure_rate`, for simulation); `refund` reverses it.
- **Ticketing** (`Ticket`): `issue` mints a ticket with a QR payload; `void`
  cancels it.
Every forward action has a compensating inverse, and the compensations are
idempotent (calling `release`/`refund`/`void` twice is safe) — which is what
makes a saga robust to retries.

**`afrotix/api.py`** — the FastAPI orchestrator (Phase 2)
The HTTP front door. It reads service DB URLs and the event-log path from the
environment (so the same code runs on SQLite or Postgres), wires up the three
services and a `SagaEngine`, optionally loads catalog stock, and exposes:
- `GET /health` — a liveness probe.
- `POST /purchase` — validates the request with a Pydantic model, runs one
  saga, and returns the outcome (saga_id, status, reason, failed_step, latency).
Run it with `uvicorn afrotix.api:app`.

### The saga — `afrotix/saga/`

**`afrotix/saga/__init__.py`** — package marker for the saga subpackage.

**`afrotix/saga/eventlog.py`** — the bronze sink
`EventLog.emit()` serializes a `SagaEvent` to one JSON object per line (JSONL)
and appends it to disk, while also keeping an in-memory list for tests and run
summaries. The file is immutable and append-only — exactly the shape Phase 3
lands in Snowflake bronze. Swapping this for a Snowflake or Kafka writer later
means changing only this one module.

**`afrotix/saga/engine.py`** — the heart of the project
`SagaEngine.purchase(...)` runs the state machine:
1. `reserve` → emit RESERVED (or, on `SoldOut`, CLOSED:compensated).
2. `charge` → emit CHARGED (or, on decline, release the hold, emit RELEASED,
   then CLOSED:compensated).
3. `issue` → emit ISSUED (or, on error, refund + release, then
   CLOSED:compensated).
4. If `cancel_after` is set (a fan cancels a *successful* purchase), unwind
   everything in reverse: void + refund + release, CLOSED:compensated with
   reason `customer_cancellation`.
5. Otherwise CLOSED:completed.
The local `emit()` and `close()` helpers write events; `close()` also stamps
`latency_ms`. The terminal `CLOSED` event carries `status`, `reason`,
`failed_step`, and `latency_ms` — the four fields that make `fct_saga_outcomes`
worth building in Phase 4. Returns a `SagaResult` dataclass.

### Scripts — `scripts/`

**`scripts/simulate.py`** — generates the event log at volume
Loads the seeded catalog, builds the three services (with a configurable
payment `failure_rate`), then fires `--runs` purchases against random customers,
tiers, and quantities, with a share cancelling after success. Writes
`data/events/saga_events.jsonl` and prints an outcome breakdown
(e.g. ~82% completed / ~13% declined / ~5% cancelled). The `--seed` flag makes
runs reproducible; `--keep` appends instead of resetting. This is the bridge to
Phase 3: it produces the raw bronze input.

### Tests — `tests/`

**`tests/test_saga.py`** — proves the saga is correct
Four tests, each backed by three throwaway SQLite databases:
- happy path completes and decrements stock,
- a declined charge releases the hold,
- a cancel-after-issue unwinds void → refund → release in order,
- an oversized request is rejected before any state changes (sold_out).
The core invariant under test: **whenever a saga ends compensated, inventory
returns to exactly where it started** — no stock leaked, no phantom holds.

### Data — `data/` (mostly gitignored)

- `data/seed/*.json` — the catalog produced by `seed.py` (committed once as the
  reference data the saga operates on).
- `data/db/*.db` — runtime SQLite databases, one per service. Generated;
  gitignored.
- `data/events/saga_events.jsonl` — the runtime event log. Generated;
  gitignored. (This becomes the Phase 3 bronze source.)

### Docs

- **`docs/LEARNING_CHECKPOINTS.md`** — the phase checklist plus a running Q&A
  capturing the *why* behind each decision (saga vs 2PC, orchestration vs
  choreography, why separate DBs, etc.). Your interview-prep cheat sheet.
- **`docs/WALKTHROUGH.md`** — this file.
- **`architecture.mermaid`** — the two-plane architecture diagram. Renders on
  GitHub and in any Mermaid viewer.

### Infra & config

- **`Dockerfile`** — packages the orchestrator: a slim Python image, installs
  requirements, and launches uvicorn on port 8000.
- **`docker-compose.yml`** — runs the orchestrator as one container with a data
  volume. Phase 7 expands this into per-service containers backed by Postgres.
- **`Makefile`** — shortcuts: `make install`, `make seed`, `make clean`.
- **`requirements.txt`** — the dependency list: faker (seed), fastapi + uvicorn
  (API), sqlalchemy (service DBs), pydantic (validation), python-dotenv (.env),
  httpx (test client), pytest (tests).
- **`.env.example`** — a template of the environment variables later phases
  need: per-service DB URLs, the simulation knobs, and Snowflake key-pair
  config. Copy to `.env` and fill in; never commit the real one.
- **`.gitignore`** — keeps generated data, runtime databases, the event log,
  caches, `.env`, and OS cruft out of the repo.
- **`README.md`** — the public front page: overview, architecture, phase plan,
  quickstart, and layout.

---

## 4. The runtime processes (how to actually run it)

1. **Seed the catalog:** `python -m afrotix.seed` → writes `data/seed/*.json`.
2. **Run the test suite:** `pytest -q` → proves the saga + every rollback path.
3. **Generate the event log:** `python -m scripts.simulate --runs 2000` →
   writes `data/events/saga_events.jsonl`.
4. **Serve the API:** `uvicorn afrotix.api:app --reload` → POST /purchase.
5. **Containerized:** `docker compose up --build`.

---

## 5. Concepts to be able to explain (interview prep)

- **Saga vs two-phase commit (2PC):** 2PC holds locks across services and
  blocks; it doesn't scale or survive long-running flows. A saga trades
  immediate consistency for *eventual* consistency via compensations — no
  cross-service locks.
- **Orchestration vs choreography:** we used orchestration — one engine drives
  the steps and owns the rollback. Easier to reason about and to emit a clean
  event log from than choreography (services reacting to each other's events).
- **Compensation & idempotency:** every undo is safe to run more than once, so
  retries and partial failures don't corrupt state.
- **Why a database per service:** isolation forces coordination through the
  saga, which is the whole point — and makes the Phase 7 split into pods
  mechanical rather than a rewrite.
- **Why an append-only event log:** it's an immutable record of history; the
  analytics plane reconstructs each saga's lifecycle from it without ever
  mutating it. This is the bronze layer of the medallion.
