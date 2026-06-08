"""AFROTIX analytics dashboard.

Reads the dbt gold marts from the local DuckDB warehouse and renders a
saga-reliability and revenue view with Streamlit + Plotly. No connectors,
no auth — it opens the warehouse file read-only.

Run from the repo root:
    pip install streamlit plotly duckdb pandas
    streamlit run dashboard/app.py

Point it at a different warehouse file with:
    AFROTIX_DUCKDB=/path/to/afrotix.duckdb streamlit run dashboard/app.py
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH: str = os.environ.get("AFROTIX_DUCKDB", "data/warehouse/afrotix.duckdb")


@st.cache_data
def query(sql: str) -> pd.DataFrame:
    """Run a read-only SQL query against the DuckDB warehouse and return a DataFrame."""
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


def main() -> None:
    """Render the AFROTIX dashboard."""
    st.set_page_config(page_title="AFROTIX Analytics", page_icon="🎟️", layout="wide")
    st.title("AFROTIX — Saga & Revenue Analytics")
    st.caption(
        "Afrobeats event ticketing on a saga (LRA) transactional core. Every "
        "purchase attempt — reserve → charge → issue, each backed by a compensating "
        "rollback — lands in the warehouse, so this answers not just *what sold*, but "
        "*where the transaction machinery breaks and what it costs*."
    )

    saga: pd.DataFrame = query("select * from main.fct_saga_outcomes")
    sales: pd.DataFrame = query("select * from main.fct_ticket_sales")

    total = len(saga)
    completed = int(saga["is_completed"].sum())
    compensated = int(saga["is_compensated"].sum())
    completion_rate = completed / total if total else 0.0
    gross = float(sales["amount"].sum())
    net = float(sales["net_amount"].sum())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Purchase Attempts", f"{total:,}")
    c2.metric("Completion Rate", f"{completion_rate:.1%}")
    c3.metric("Gross Revenue", f"${gross:,.0f}")
    c4.metric("Net Revenue", f"${net:,.0f}")

    st.divider()
    st.header("Saga reliability")
    st.write(
        "A purchase is a distributed transaction: reserve → charge → issue, each "
        "with a compensating undo (release → refund → void). When a step fails, the "
        "saga rolls back and closes as `compensated` with a reason."
    )

    k1, k2, k3 = st.columns(3)
    k1.metric("Compensated (rolled back)", f"{compensated:,}")
    k2.metric("Completion Rate", f"{completion_rate:.1%}")
    k3.metric("Avg Saga Latency (ms)", f"{saga['latency_ms'].mean():,.1f}")

    left, right = st.columns(2)
    by_status = (
        saga.groupby("status", as_index=False).size()
        .rename(columns={"size": "sagas"}).sort_values("sagas")
    )
    left.plotly_chart(
        px.bar(by_status, x="sagas", y="status", orientation="h",
               title="Outcomes by Status"),
        use_container_width=True,
    )
    reasons = (
        saga[saga["is_compensated"]].groupby("reason", as_index=False).size()
        .rename(columns={"size": "sagas"}).sort_values("sagas")
    )
    right.plotly_chart(
        px.bar(reasons, x="sagas", y="reason", orientation="h",
               title="Why Sagas Compensated"),
        use_container_width=True,
    )

    steps = (
        saga[saga["is_compensated"] & saga["failed_step"].notna()]
        .groupby("failed_step", as_index=False).size()
        .rename(columns={"size": "sagas"}).sort_values("failed_step")
    )
    steps["failed_step"] = "step " + steps["failed_step"].astype(int).astype(str)
    daily = saga.groupby(["date_day", "status"], as_index=False).size().rename(
        columns={"size": "sagas"})
    s_left, s_right = st.columns(2)
    s_left.plotly_chart(
        px.bar(steps, x="failed_step", y="sagas", title="Which Step Failed"),
        use_container_width=True,
    )
    s_right.plotly_chart(
        px.line(daily, x="date_day", y="sagas", color="status",
                title="Daily Outcomes by Status"),
        use_container_width=True,
    )

    st.divider()
    st.header("Revenue")
    st.write(
        "Revenue comes from issued tickets. `gross` counts every issue; `net` zeroes "
        "out cancellations — the gap is revenue lost to rollbacks."
    )

    lost = gross - net
    cancel_rate = float(sales["is_cancelled"].mean()) if len(sales) else 0.0
    r1, r2, r3 = st.columns(3)
    r1.metric("Lost to Cancellations", f"${lost:,.0f}")
    r2.metric("Tickets Issued", f"{len(sales):,}")
    r3.metric("Cancellation Rate", f"{cancel_rate:.1%}")

    rev_day = (
        sales.groupby("date_day", as_index=False)[["amount", "net_amount"]].sum()
        .rename(columns={"amount": "gross", "net_amount": "net"})
    )
    st.plotly_chart(
        px.line(rev_day, x="date_day", y=["gross", "net"],
                title="Gross vs Net Revenue by Day"),
        use_container_width=True,
    )
    by_tier = (
        sales.groupby("tier_id", as_index=False)["net_amount"].sum()
        .sort_values("net_amount", ascending=False).head(15)
    )
    st.plotly_chart(
        px.bar(by_tier, x="tier_id", y="net_amount", title="Top Tiers by Net Revenue"),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
