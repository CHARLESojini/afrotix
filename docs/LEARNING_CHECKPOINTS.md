# AFROTIX — Learning Checkpoints

A running Q&A log across build phases, so the *why* behind each decision stays
captured (same habit as the stock-pipeline-k8s tracker).

## Phase progress
- [x] **Phase 0** — Architecture & scaffold
- [x] **Phase 1** — Data model + Faker seed
- [x] **Phase 2** — Saga orchestrator (FastAPI, complete/compensate/after)
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

---

## Phase 2 — Saga orchestrator

**Q: What's the one rule that makes a saga correct?**
A: If a later step fails, undo the earlier steps in *reverse order*. Analogy: a
bartender pours the drink, runs your card, hands it over; if the card declines
after pouring, they tip the drink back. Forward: reserve → charge → issue.
Undo: void → refund → release.

**Q: Why give each service its own database?**
A: Service isolation. Inventory can't reach into Payments' tables, so the only
way to coordinate them is the saga + compensations — which is the whole point of
the pattern, and what makes the Phase 7 split into separate pods mechanical.

**Q: Orchestration vs choreography — which did we use and why?**
A: Orchestration. One engine (`SagaEngine`) explicitly calls each step and owns
the compensation logic. It's easier to reason about and to emit a clean event
log from than choreography (services reacting to each other's events), which we
could revisit if we add Kafka in Phase 7.

**Q: Why is the CLOSED event ('after') so important for the DE side?**
A: It carries the terminal `status`, the `reason` for any compensation, the
`failed_step`, and `latency_ms`. That single event is what `fct_saga_outcomes`
is built from — success rate, failure reasons, and where the machinery breaks.

**Q: Why simulate a payment-failure rate and a cancel rate?**
A: A log that's 100% happy-path is useless for analytics. The ~12% declines and
~5% cancellations create the variation that makes the gold marts tell a story.

**Q: Why keep an in-memory list AND write JSONL?**
A: The JSONL file is the durable bronze artifact; the in-memory list is a
convenience for tests and the run summary. In Phase 3 the file (or a Kafka
topic) becomes the actual bronze source.
