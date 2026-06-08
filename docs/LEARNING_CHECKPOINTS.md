# AFROTIX — Learning Checkpoints

A running Q&A log across build phases, so the *why* behind each decision stays
captured (same habit as the stock-pipeline-k8s tracker).

## Phase progress
- [x] **Phase 0** — Architecture & scaffold
- [x] **Phase 1** — Data model + Faker seed
- [x] **Phase 2** — Saga orchestrator (FastAPI, complete/compensate/after)
- [x] **Phase 3** — Terminal events → Snowflake bronze
- [x] **Phase 4** — dbt silver + gold marts (incl. `fct_saga_outcomes`)
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

---

## Phase 3 — Bronze ingestion

**Q: What is the "bronze" layer and what rule governs it?**
A: The raw landing zone of the medallion. Rule: minimally transformed, append-
only, never rewritten. You keep everything as it arrived so you can always
reprocess downstream without re-ingesting.

**Q: Why build against an adapter interface instead of just writing to Snowflake?**
A: Cost and speed. DuckDB gives a free, instant local dev loop; Snowflake is the
production target. Same loader, same SQL surface — `WAREHOUSE=snowflake` flips
it. The loader is a delivery driver who only knows the loading dock; the two
warehouses look identical from there.

**Q: How is the load idempotent if bronze is append-only (no deletes)?**
A: Each event gets a deterministic SHA-256 `_row_hash`. The loader reads the
hashes already present and inserts only new ones, so re-running adds nothing.
`--full-refresh` is the escape hatch that truncates and rebuilds.

**Q: Why store typed columns AND a JSON/VARIANT payload?**
A: The typed columns (saga_id, event_type, occurred_at, ...) make silver easy to
write; the `payload` keeps the nested, event-specific bits (hold_id, reason,
latency_ms) without forcing a schema on them at ingest time.

**Q: What ingestion metadata do we add, and why?**
A: `_row_hash` (idempotency), `_source_file` and `_batch_id` (lineage — which
run produced a row), `_loaded_at` (when). This is standard bronze provenance.

**Q: Why DuckDB specifically for local dev?**
A: It's an in-process analytical database (think "SQLite for analytics"), reads
JSON natively, and speaks SQL close enough to Snowflake that dbt models built on
one mostly run on the other.

---

## Phase 4 — dbt silver + gold marts

**Q: What does each medallion layer do here?**
A: Bronze = raw landing (events + catalog, loaded outside dbt). Silver = cleaned
and reshaped (`stg_saga_events` types + lifts payload fields; `int_saga_lifecycle`
pivots the stream to one row per saga). Gold = business-ready marts
(`fct_saga_outcomes`, `fct_ticket_sales`, the dims).

**Q: Why pivot the event stream into one row per saga?**
A: Bronze has many rows per purchase (reserved, charged, issued, closed...).
Analysis wants one row per *attempt* with its outcome, timestamps, and latency.
`int_saga_lifecycle` does that with conditional aggregation
(`max(case when event_type = 'closed' then status end)`), which is the standard
event-stream-to-entity pivot.

**Q: How do the same models run on both DuckDB and Snowflake?**
A: The only dialect difference is pulling fields out of the JSON/VARIANT payload.
That lives in one macro, `extract_json`, which branches on `target.type`
(`payload->>'k'` for DuckDB, `payload:k::string` for Snowflake). Every model
calls the macro, so flipping the warehouse changes nothing in the SQL.

**Q: Why is `fct_saga_outcomes` the centerpiece?**
A: It's the mart that only exists because we routed saga outcomes into the
warehouse: success rate, failure reasons, which step broke, and latency — the
reliability story most ticketing analytics never capture.

**Q: Why does `fct_ticket_sales` carry both `amount` and `net_amount`?**
A: A ticket can be issued and later cancelled. `amount` is gross; `net_amount`
zeroes out cancellations, so gross vs net revenue both come straight from one
table (gross $466k vs net $442k in the sample run).

**Q: Why are the catalog dims loaded separately from events?**
A: Two ingestion paths by design — events are append-only facts (load_bronze),
the catalog is reference data (load_catalog). Both land in bronze; dbt builds
conformed dimensions from the catalog tables.

**Q: Why staging as views but marts as tables?**
A: Staging is light and always-fresh, so views avoid storage and staleness;
marts are queried repeatedly by dashboards, so materializing them as tables
makes reads fast. Set once in `dbt_project.yml`.

---

## Phase 5 — Dagster orchestration

**Q: What does Dagster add over running the scripts by hand?**
A: Scheduling, lineage, retries, observability. The scripts still do the work;
Dagster is air-traffic control — sequences them, watches each finish, alerts on
failure.

**Q: Why model steps as assets instead of tasks/ops?**
A: Assets model the things produced (bronze tables, marts), not just actions, so
Dagster tracks each one's lineage and freshness — the medallion as a graph of
data.

**Q: How does the same graph target DuckDB or Snowflake?**
A: The assets shell out to the same CLI and inherit WAREHOUSE/DBT_TARGET from the
environment; source .env before `dagster dev` and the whole graph follows.

**Q: What was the warehouse-split bug?**
A: The loaders auto-read .env (WAREHOUSE) but dbt doesn't, so DBT_TARGET must be
set too — and DUCKDB_PATH must not be a single relative path, since the loaders
run from the repo root and dbt from transform/.

---

## Phase 6 — Streamlit dashboard

**Q: Why Streamlit reading DuckDB instead of a BI connector?**
A: A BI tool sits between you and the warehouse with its own auth/connection
layer (the part that fought us). Streamlit is just Python opening the DuckDB
file read-only — no connector, no key encoding, no path resolution. dbt already
produced the marts; the app is a thin read-only presentation layer over them.

**Q: Why @st.cache_data on the query function?**
A: Streamlit re-runs the whole script top-to-bottom on every interaction.
Without caching, each rerun re-hits DuckDB. The decorator memoizes by SQL string
so identical queries return the cached DataFrame.

**Q: Why did the numbers read double (4000/702) at first?**
A: simulate appends to data/events/saga_events.jsonl. Running it twice without
deleting the log doubled every saga. Rates were unaffected (still 82.5%), but
absolute counts and revenue were 2x. Fix: rm the event log before re-simulating,
then rebuild. Canonical truth: 1649 completed / 351 compensated.
