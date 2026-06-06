"""Dagster Definitions for the AFROTIX pipeline.

Launch the UI from the repo root (after sourcing .env):

    dagster dev -m orchestration.definitions

Then materialize all assets once, or let the daily schedule run the whole
medallion: seed -> simulate -> bronze (events + catalog) -> dbt marts.
"""
from __future__ import annotations

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)

from orchestration import assets

all_assets = load_assets_from_modules([assets])

# One job that materializes the whole medallion in dependency order.
afrotix_pipeline_job = define_asset_job(
    name="afrotix_pipeline",
    selection=AssetSelection.all(),
)

# Daily morning batch window (06:30). Adjust the cron to taste.
daily_schedule = ScheduleDefinition(
    name="afrotix_daily",
    job=afrotix_pipeline_job,
    cron_schedule="30 6 * * *",
)

defs = Definitions(
    assets=all_assets,
    jobs=[afrotix_pipeline_job],
    schedules=[daily_schedule],
)
