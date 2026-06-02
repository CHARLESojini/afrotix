# AFROTIX — Learning Checkpoints

A running Q&A log across build phases, so the *why* behind each decision stays
captured (same habit as the stock-pipeline-k8s tracker).

## Phase progress
- [x] **Phase 0** — Architecture & scaffold
- [ ] **Phase 1** — Data model + Faker seed
- [ ] **Phase 2** — Saga orchestrator (FastAPI, complete/compensate/after)
- [ ] **Phase 3** — Terminal events → Snowflake bronze
- [ ] **Phase 4** — dbt silver + gold marts (incl. `fct_saga_outcomes`)
- [ ] **Phase 5** — Dagster orchestration
- [ ] **Phase 6** — Evidence dashboard + saga-failure analysis
- [ ] **Phase 7** — (stretch) Kafka bus + EKS/Terraform/Helm deploy

---

## Phase 0 — Architecture & scaffold

**Q: What is the saga / LRA pattern in one line?**
A: A distributed transaction broken into a sequence of local steps, each with a
compensating "undo," giving eventual consistency without holding a lock across
services. Analogy: booking a holiday — flight, then hotel, then car; if the
hotel is full after you paid for the flight, you cancel (compensate) the flight.

**Q: How do the article's three LRA callbacks map to AFROTIX?**
A:
- `complete` → confirm the sale: **reserve → charge → issue**
- `compensate` → run the undos: **release → refund → void**
- `after` → emit the terminal event into bronze once the saga finalizes
  (this is the hook that bridges the transactional and analytics planes)

**Q: Why route saga outcomes into a medallion warehouse instead of just a DB?**
A: Every outcome becomes analytical data. `fct_saga_outcomes` lets us ask where
sagas fail, how often they compensate, and how long they take — distributed-
systems observability through a data lens, which is the portfolio differentiator.

**Q: Why is the bronze event log append-only?**
A: It is an immutable record of every state transition. Silver reconstructs one
row per `saga_id` from that log (`int_saga_lifecycle`); we never mutate history.

**Q: Why generate the catalog (dimensions) separately from saga events?**
A: Clean separation of reference data (events/artists/venues/customers/tiers)
from transactional facts. The seed builds the catalog the saga *operates on*;
the saga itself produces the facts in Phase 2.

**Q: Why normalized JSON files with foreign keys (not nested)?**
A: They map straight onto warehouse tables, so landing them in Snowflake later
is trivial. `events.json` carries `artist_id`/`venue_id`; `ticket_tiers.json`
carries `event_id`.
