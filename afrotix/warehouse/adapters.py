"""Warehouse adapters: one bronze loader, two interchangeable targets.

The BronzeLoader talks to a WarehouseAdapter, so switching from local DuckDB to
Snowflake is a config change (``WAREHOUSE=snowflake``), not a code change. Both
adapters expose the same small surface: create the bronze table if needed,
report which row-hashes are already loaded (for idempotency), append rows,
truncate, and count.

Analogy: the loader is a delivery driver who only knows "drop the boxes at the
loading dock." DuckDB and Snowflake are two different warehouses, but the dock
(this interface) looks identical from the driver's seat.
"""
from __future__ import annotations

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Sequence, Set

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
BRONZE_TABLE = "raw_saga_events"
FQ_TABLE = f"{BRONZE_SCHEMA}.{BRONZE_TABLE}"

# Shared column order: (name, duckdb_type, snowflake_type). The leading columns
# mirror the SagaEvent; the underscore-prefixed ones are ingestion metadata.
COLUMNS = [
    ("saga_id", "VARCHAR", "VARCHAR"),
    ("event_type", "VARCHAR", "VARCHAR"),
    ("step", "INTEGER", "NUMBER"),
    ("customer_id", "VARCHAR", "VARCHAR"),
    ("event_id", "VARCHAR", "VARCHAR"),
    ("tier_id", "VARCHAR", "VARCHAR"),
    ("amount", "DOUBLE", "FLOAT"),
    ("occurred_at", "TIMESTAMP", "TIMESTAMP_NTZ"),
    ("payload", "JSON", "VARIANT"),
    ("_row_hash", "VARCHAR", "VARCHAR"),
    ("_source_file", "VARCHAR", "VARCHAR"),
    ("_batch_id", "VARCHAR", "VARCHAR"),
    ("_loaded_at", "TIMESTAMP", "TIMESTAMP_NTZ"),
]
COLUMN_NAMES: List[str] = [c[0] for c in COLUMNS]


class WarehouseAdapter(ABC):
    """The loading-dock interface every warehouse target implements."""

    @abstractmethod
    def ensure_schema(self) -> None:
        """Create the bronze schema and table if they do not exist."""

    @abstractmethod
    def existing_hashes(self) -> Set[str]:
        """Return the set of ``_row_hash`` values already in the table."""

    @abstractmethod
    def insert(self, rows: Sequence[Dict]) -> int:
        """Append rows (each a dict keyed by COLUMN_NAMES); return the count."""

    @abstractmethod
    def truncate(self) -> None:
        """Remove all rows (used for a full refresh)."""

    @abstractmethod
    def count(self) -> int:
        """Return the current row count."""

    @abstractmethod
    def close(self) -> None:
        """Release the connection."""


class DuckDBAdapter(WarehouseAdapter):
    """Local DuckDB target — fast, free, file-based; the default for dev."""

    def __init__(self, path: str) -> None:
        import duckdb  # local import keeps the dependency optional

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._con = duckdb.connect(path)
        self._ddl_types = {name: dk for name, dk, _ in COLUMNS}

    def replace_table_from_json(self, table: str, json_path: str) -> int:
        """Load a JSON-array file into bronze.<table>, replacing it.

        DuckDB reads JSON natively, so a catalog dimension lands in one
        statement with inferred column types.
        """
        self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        self._con.execute(
            f"CREATE OR REPLACE TABLE {BRONZE_SCHEMA}.{table} AS "
            f"SELECT * FROM read_json_auto(?)",
            [json_path],
        )
        return self._con.execute(
            f"SELECT COUNT(*) FROM {BRONZE_SCHEMA}.{table}"
        ).fetchone()[0]

    def ensure_schema(self) -> None:
        cols = ", ".join(f"{name} {self._ddl_types[name]}" for name in COLUMN_NAMES)
        self._con.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        self._con.execute(f"CREATE TABLE IF NOT EXISTS {FQ_TABLE} ({cols})")

    def existing_hashes(self) -> Set[str]:
        rows = self._con.execute(f"SELECT _row_hash FROM {FQ_TABLE}").fetchall()
        return {r[0] for r in rows}

    def insert(self, rows: Sequence[Dict]) -> int:
        if not rows:
            return 0
        placeholders = ", ".join(
            "?::JSON" if name == "payload" else "?" for name in COLUMN_NAMES
        )
        sql = f"INSERT INTO {FQ_TABLE} ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders})"
        values = [tuple(row[name] for name in COLUMN_NAMES) for row in rows]
        self._con.executemany(sql, values)
        return len(values)

    def truncate(self) -> None:
        self._con.execute(f"DELETE FROM {FQ_TABLE}")

    def count(self) -> int:
        return self._con.execute(f"SELECT COUNT(*) FROM {FQ_TABLE}").fetchone()[0]

    def close(self) -> None:
        self._con.close()


class SnowflakeAdapter(WarehouseAdapter):
    """Snowflake target — the production warehouse. Wired, ready to flip.

    Credentials and target come from the environment (see .env.example). The
    snowflake connector is imported lazily so local DuckDB work needs nothing
    installed for Snowflake.
    """

    def __init__(self) -> None:
        import snowflake.connector  # lazy: only needed when WAREHOUSE=snowflake

        connect_kwargs = dict(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            role=os.getenv("SNOWFLAKE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=BRONZE_SCHEMA,
        )
        key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
        if key_path:
            connect_kwargs["private_key_file"] = key_path
        else:
            connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
        self._con = snowflake.connector.connect(**connect_kwargs)
        self._ddl_types = {name: sf for name, _, sf in COLUMNS}

    def replace_table_from_json(self, table: str, json_path: str) -> int:
        """Load a JSON-array file into bronze.<table>, replacing it.

        Reads records in Python and inserts them with an inferred typed schema.
        Fine for catalog-sized reference data; for very large sets prefer a
        stage + COPY.
        """
        import json as _json

        records = _json.loads(Path(json_path).read_text())
        if not records:
            return 0
        keys = list(records[0].keys())

        def _sf_type(value: object) -> str:
            if isinstance(value, bool):
                return "BOOLEAN"
            if isinstance(value, int):
                return "NUMBER"
            if isinstance(value, float):
                return "FLOAT"
            return "VARCHAR"

        coldefs = ", ".join(f"{k} {_sf_type(records[0][k])}" for k in keys)
        cur = self._con.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        cur.execute(f"CREATE OR REPLACE TABLE {BRONZE_SCHEMA}.{table} ({coldefs})")
        placeholders = ", ".join(["%s"] * len(keys))
        cur.executemany(
            f"INSERT INTO {BRONZE_SCHEMA}.{table} ({', '.join(keys)}) VALUES ({placeholders})",
            [tuple(r.get(k) for k in keys) for r in records],
        )
        cur.close()
        return len(records)

    def ensure_schema(self) -> None:
        cols = ", ".join(f"{name} {self._ddl_types[name]}" for name in COLUMN_NAMES)
        cur = self._con.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
        cur.execute(f"CREATE TABLE IF NOT EXISTS {FQ_TABLE} ({cols})")
        cur.close()

    def existing_hashes(self) -> Set[str]:
        cur = self._con.cursor()
        cur.execute(f"SELECT _row_hash FROM {FQ_TABLE}")
        result = {r[0] for r in cur.fetchall()}
        cur.close()
        return result

    def insert(self, rows: Sequence[Dict]) -> int:
        if not rows:
            return 0
        # VARIANT columns require PARSE_JSON, so build an INSERT ... SELECT.
        selects = ", ".join(
            "PARSE_JSON(%s)" if name == "payload" else "%s" for name in COLUMN_NAMES
        )
        sql = (
            f"INSERT INTO {FQ_TABLE} ({', '.join(COLUMN_NAMES)}) "
            f"SELECT {selects}"
        )
        values = [tuple(row[name] for name in COLUMN_NAMES) for row in rows]
        cur = self._con.cursor()
        cur.executemany(sql, values)
        cur.close()
        return len(values)

    def truncate(self) -> None:
        cur = self._con.cursor()
        cur.execute(f"TRUNCATE TABLE IF EXISTS {FQ_TABLE}")
        cur.close()

    def count(self) -> int:
        cur = self._con.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {FQ_TABLE}")
        n = cur.fetchone()[0]
        cur.close()
        return n

    def close(self) -> None:
        self._con.close()


def get_adapter() -> WarehouseAdapter:
    """Return the adapter named by the ``WAREHOUSE`` env var (default duckdb)."""
    target = os.getenv("WAREHOUSE", "duckdb").lower()
    if target == "duckdb":
        return DuckDBAdapter(os.getenv("DUCKDB_PATH", "data/warehouse/afrotix.duckdb"))
    if target == "snowflake":
        return SnowflakeAdapter()
    raise ValueError(f"Unknown WAREHOUSE target: {target!r} (use duckdb or snowflake)")
