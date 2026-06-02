# AFROTIX

Afrobeats event ticketing built around a **saga (LRA) transactional core** whose
every outcome flows into a **medallion analytics layer** on Snowflake.

Buying a ticket is a distributed transaction: **reserve → charge → issue**, each
step with a compensating undo (**release → refund → void**). Rather than ending
there, every saga outcome is emitted as an event and modeled in dbt — so the
project answers not just *what sold*, but *where the transaction machinery breaks
and what it costs*.

## Architecture

See `architecture.mermaid`. Two planes:

- **Transactional plane (saga / LRA):** a FastAPI orchestrator coordinating
  Inventory, Payments, and Ticketing services, each owning its own database.
- **Analytics plane (medallion):** terminal saga events land in Snowflake
  **bronze**, dbt builds **silver** (`int_saga_lifecycle`) and **gold**
  (`fct_ticket_sales`, `fct_saga_outcomes`, dims), surfaced in Evidence.
  Dagster orchestrates ingestion and dbt runs.

## Phase plan
0. Architecture & scaffold ← **you are here**
1. Data model + Faker seed
2. Saga orchestrator (complete / compensate / after)
3. Terminal events → Snowflake bronze
4. dbt silver + gold marts
5. Dagster orchestration
6. Evidence dashboard + saga-failure analysis
7. (stretch) Kafka event bus + Kubernetes deploy

## Quickstart
```bash
make install        # or: pip install -r requirements.txt
make seed           # writes data/seed/*.json
```
Tune volumes:
```bash
python -m afrotix.seed --artists 30 --venues 15 --customers 1000 --events 80
```

## Layout
```
afrotix/
├── afrotix/                # package
│   ├── models.py           # catalog entities + bronze saga-event contract
│   └── seed.py             # Faker catalog seeder (Phase 1)
├── data/seed/              # generated JSON (gitignored in practice)
├── docs/LEARNING_CHECKPOINTS.md
├── architecture.mermaid
├── .env.example
├── Makefile
└── requirements.txt
```
