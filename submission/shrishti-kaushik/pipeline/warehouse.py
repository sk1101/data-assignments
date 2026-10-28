"""Warehouse: SCD2 history tables and current views in DuckDB, built only from
the lake. See docs/05_warehouse.md.
"""

import json
from pathlib import Path

import duckdb

from pipeline.lake import read_lake
from pipeline.timeutil import utc_now

TYPE_MAP = {"INTEGER": "BIGINT", "TEXT": "VARCHAR"}

META_COLUMNS = [
    ("_lsn", "BIGINT NOT NULL"),
    ("_op", "VARCHAR NOT NULL"),
    ("_is_deleted", "BOOLEAN NOT NULL"),
    ("_valid_from", "VARCHAR NOT NULL"),
    ("_valid_to", "VARCHAR"),
    ("_valid_to_lsn", "BIGINT"),
]

VISIBLE_AT_LSN = "_lsn <= {lsn} AND (_valid_to_lsn IS NULL OR _valid_to_lsn > {lsn})"


def connect_warehouse(path: Path | str = ":memory:") -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(path))


# ── Structure ────────────────────────────────────────────────────────────────


def create_warehouse(conn: duckdb.DuckDBPyConnection, contract: dict) -> None:
    """Create the history tables, current views and watermark (safe to re-run)."""
    for schema in ("history", "curated", "meta"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    conn.execute(
        "CREATE TABLE IF NOT EXISTS meta.applied (last_lsn BIGINT NOT NULL, updated_at VARCHAR)"
    )
    if conn.execute("SELECT COUNT(*) FROM meta.applied").fetchone()[0] == 0:
        conn.execute("INSERT INTO meta.applied VALUES (0, ?)", [utc_now()])

    for table, spec in contract["tables"].items():
        source_cols = [f"{name} {TYPE_MAP[d['type']]}" for name, d in spec["columns"].items()]
        meta_cols = [f"{name} {definition}" for name, definition in META_COLUMNS]
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS history.{table} ({', '.join(source_cols + meta_cols)})"
        )
        conn.execute(
            f"CREATE OR REPLACE VIEW curated.{table} AS "
            f"SELECT {', '.join(spec['columns'])} FROM history.{table} "
            "WHERE _valid_to_lsn IS NULL AND NOT _is_deleted"
        )


def applied_lsn(conn: duckdb.DuckDBPyConnection) -> int:
    return conn.execute("SELECT last_lsn FROM meta.applied").fetchone()[0]


# ── Applying changes ─────────────────────────────────────────────────────────


def _apply_change(conn: duckdb.DuckDBPyConnection, contract: dict, change: dict) -> None:
    """Close the row's current version, then add the new version (or tombstone)."""
    table = change["table_name"]
    columns = list(contract["tables"][table]["columns"])
    key = json.loads(change["pk"])
    row = json.loads(change["after"] or change["before"])

    key_match = " AND ".join(f"{col} = ?" for col in key)
    conn.execute(
        f"UPDATE history.{table} SET _valid_to = ?, _valid_to_lsn = ? "
        f"WHERE {key_match} AND _valid_to_lsn IS NULL",
        [change["captured_at"], change["lsn"], *key.values()],
    )

    all_cols = columns + [name for name, _ in META_COLUMNS]
    marks = ", ".join("?" for _ in all_cols)
    conn.execute(
        f"INSERT INTO history.{table} ({', '.join(all_cols)}) VALUES ({marks})",
        [
            *(row.get(col) for col in columns),
            change["lsn"],
            change["op"],
            change["op"] == "D",
            change["captured_at"],
            None,
            None,
        ],
    )


def apply_changes(
    conn: duckdb.DuckDBPyConnection,
    contract: dict,
    lake_dir: Path,
    up_to_lsn: int | None = None,
) -> int:
    """Apply lake changes above the watermark (optionally only up to an lsn).

    Duplicates are applied once, in lsn order, and the whole batch plus the
    watermark commit in one transaction: a crash leaves the warehouse untouched.
    """
    watermark = applied_lsn(conn)
    changes = read_lake(lake_dir, after_lsn=watermark)
    if up_to_lsn is not None:
        changes = [c for c in changes if c["lsn"] <= up_to_lsn]

    # Each lsn is one change: keep a single copy (the lake is already in lsn order).
    unique = list({change["lsn"]: change for change in changes}.values())
    if not unique:
        return 0

    conn.begin()
    try:
        for change in unique:
            _apply_change(conn, contract, change)
        conn.execute(
            "UPDATE meta.applied SET last_lsn = ?, updated_at = ?",
            [unique[-1]["lsn"], utc_now()],
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return len(unique)


# ── Reading, time travel and restore ─────────────────────────────────────────


def current(conn: duckdb.DuckDBPyConnection, contract: dict, table: str) -> list[dict]:
    """The current state of a table (from its curated view), ordered by key."""
    spec = contract["tables"][table]
    cols = list(spec["columns"])
    rows = conn.execute(
        f"SELECT {', '.join(cols)} FROM curated.{table} ORDER BY {', '.join(spec['primary_key'])}"
    ).fetchall()
    return [dict(zip(cols, row, strict=True)) for row in rows]


def as_of(conn: duckdb.DuckDBPyConnection, contract: dict, table: str, lsn: int) -> list[dict]:
    """A table exactly as it was right after the change with this lsn."""
    spec = contract["tables"][table]
    cols = list(spec["columns"])
    rows = conn.execute(
        f"SELECT {', '.join(cols)} FROM history.{table} "
        f"WHERE {VISIBLE_AT_LSN.format(lsn=int(lsn))} AND NOT _is_deleted "
        f"ORDER BY {', '.join(spec['primary_key'])}"
    ).fetchall()
    return [dict(zip(cols, row, strict=True)) for row in rows]


def lsn_at(conn: duckdb.DuckDBPyConnection, contract: dict, timestamp: str) -> int:
    """The last lsn captured at or before a UTC timestamp (YYYY-MM-DDTHH:MM:SS.sssZ)."""
    union = " UNION ALL ".join(
        f"SELECT _lsn, _valid_from FROM history.{table}" for table in contract["tables"]
    )
    query = f"SELECT COALESCE(MAX(_lsn), 0) FROM ({union}) WHERE _valid_from <= ?"
    return conn.execute(query, [timestamp]).fetchone()[0]


def restore_point(conn: duckdb.DuckDBPyConnection, contract: dict, lsn: int) -> str:
    """Materialise every table as of an lsn into schema restore_lsn_<lsn>."""
    schema = f"restore_lsn_{int(lsn)}"
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for table, spec in contract["tables"].items():
        conn.execute(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS "
            f"SELECT {', '.join(spec['columns'])} FROM history.{table} "
            f"WHERE {VISIBLE_AT_LSN.format(lsn=int(lsn))} AND NOT _is_deleted"
        )
    return schema


def rebuild(
    conn: duckdb.DuckDBPyConnection, contract: dict, lake_dir: Path, up_to_lsn: int | None = None
) -> int:
    """Drop the warehouse and replay the lake (optionally only up to an lsn)."""
    for schema in ("curated", "history", "meta"):
        conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    create_warehouse(conn, contract)
    return apply_changes(conn, contract, lake_dir, up_to_lsn=up_to_lsn)
