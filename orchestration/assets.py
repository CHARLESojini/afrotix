"""Dagster assets that orchestrate the AFROTIX medallion pipeline.

Each asset wraps one step of the pipeline and shells out to the same CLI you
run by hand, so this layer adds scheduling, lineage, and observability without
duplicating any logic. The dependency edges form the medallion DAG:

    catalog_seed ─┬─► saga_events ─► bronze_saga_events ─┐
                  └─► bronze_catalog ────────────────────┴─► dbt_marts

Analogy: Dagster is air-traffic control. The planes (your scripts) already know
how to fly; Dagster decides the take-off order, watches each one land, and
raises an alarm if one goes down — so you stop being the control tower yourself.

Warehouse selection is inherited from the environment (WAREHOUSE / DBT_TARGET),
exactly as when you run the steps by hand, so `dagster dev` after sourcing your
.env targets DuckDB or Snowflake with no code change.
"""
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

# Repo root is one level up from this package (orchestration/ -> repo root).
REPO_ROOT = Path(__file__).resolve().parent.parent

# How many purchases the simulation drives; override with AFROTIX_SIMULATE_RUNS.
SIMULATE_RUNS = int(os.getenv("AFROTIX_SIMULATE_RUNS", "2000"))


def _run(context: AssetExecutionContext, cmd: List[str], cwd: Path = REPO_ROOT) -> str:
    """Run a pipeline command, log its output, and fail the asset on non-zero exit.

    Returns captured stdout so callers can scrape metadata from it.
    """
    context.log.info(f"$ {' '.join(cmd)}  (cwd={cwd})")
    proc = subprocess.run(
        cmd, cwd=cwd, capture_output=True, text=True, env=os.environ.copy()
    )
    if proc.stdout:
        context.log.info(proc.stdout)
    if proc.returncode != 0:
        context.log.error(proc.stderr or "(no stderr)")
        raise RuntimeError(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout


def _scrape_int(text: str, pattern: str) -> Optional[int]:
    """Return the first integer captured by ``pattern`` in ``text``, or None."""
    match = re.search(pattern, text)
    return int(match.group(1)) if match else None


@asset(group_name="catalog")
def catalog_seed(context: AssetExecutionContext) -> MaterializeResult:
    """Generate the reference catalog: artists, venues, customers, events, tiers."""
    out = _run(context, ["python", "-m", "afrotix.seed"])
    return MaterializeResult(metadata={"log_tail": MetadataValue.text(out[-500:])})


@asset(deps=[catalog_seed], group_name="saga")
def saga_events(context: AssetExecutionContext) -> MaterializeResult:
    """Run the purchase saga simulation, appending outcomes to the JSONL event log."""
    out = _run(context, ["python", "-m", "scripts.simulate", "--runs", str(SIMULATE_RUNS)])
    return MaterializeResult(
        metadata={"runs": SIMULATE_RUNS, "log_tail": MetadataValue.text(out[-500:])}
    )


@asset(deps=[saga_events], group_name="bronze")
def bronze_saga_events(context: AssetExecutionContext) -> MaterializeResult:
    """Load the saga event log into the warehouse bronze layer (full refresh)."""
    out = _run(context, ["python", "-m", "scripts.load_bronze", "--full-refresh"])
    metadata: Dict[str, object] = {"warehouse": os.getenv("WAREHOUSE", "duckdb")}
    inserted = _scrape_int(out, r"inserted=(\d+)")
    total = _scrape_int(out, r"total_in_bronze=(\d+)")
    if inserted is not None:
        metadata["inserted"] = inserted
    if total is not None:
        metadata["total_in_bronze"] = total
    return MaterializeResult(metadata=metadata)


@asset(deps=[catalog_seed], group_name="bronze")
def bronze_catalog(context: AssetExecutionContext) -> MaterializeResult:
    """Load the reference catalog into the warehouse bronze raw_* tables."""
    out = _run(context, ["python", "-m", "scripts.load_catalog"])
    return MaterializeResult(
        metadata={
            "warehouse": os.getenv("WAREHOUSE", "duckdb"),
            "log_tail": MetadataValue.text(out[-500:]),
        }
    )


@asset(deps=[bronze_saga_events, bronze_catalog], group_name="marts")
def dbt_marts(context: AssetExecutionContext) -> MaterializeResult:
    """Build the dbt silver + gold marts on top of the bronze layer."""
    out = _run(context, ["dbt", "build", "--profiles-dir", "."], cwd=REPO_ROOT / "transform")
    metadata: Dict[str, object] = {"dbt_target": os.getenv("DBT_TARGET", "duckdb")}
    passed = _scrape_int(out, r"PASS=(\d+)")
    if passed is None:
        passed = _scrape_int(out, r"(\d+)\s+success")
    if passed is not None:
        metadata["passed"] = passed
    metadata["log_tail"] = MetadataValue.text(out[-800:])
    return MaterializeResult(metadata=metadata)
