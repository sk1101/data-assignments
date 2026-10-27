"""CDC capture: installs the change log (and, next, its triggers) on the source.

Simulates log-based CDC; see docs/02_cdc_contract.md. The source team owns
source/schema.sql. This module is the data platform's CDC "connector".
"""

import json
import sqlite3
from dataclasses import dataclass

CDC_LOG = "_cdc_log"

CDC_LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {CDC_LOG} (
    lsn INTEGER PRIMARY KEY AUTOINCREMENT,
    table_name TEXT NOT NULL,
    op TEXT NOT NULL CHECK (op IN ('I', 'U', 'D')),
    pk TEXT NOT NULL,
    before TEXT,
    after TEXT,
    captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK ((op = 'I') = (before IS NULL)),
    CHECK ((op = 'D') = (after IS NULL))
) STRICT
"""


@dataclass(frozen=True)
class ChangeRecord:
    """One captured change, as defined in docs/02_cdc_contract.md."""

    lsn: int
    table: str
    op: str
    pk: dict
    before: dict | None
    after: dict | None
    captured_at: str


def source_tables(conn: sqlite3.Connection) -> list[str]:
    """Business tables to capture: everything except SQLite internals and the log."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' "
        "AND name NOT LIKE 'sqlite_%' AND name <> ? ORDER BY name",
        (CDC_LOG,),
    ).fetchall()
    return [name for (name,) in rows]


_TRIGGER_TEMPLATE = """
CREATE TRIGGER IF NOT EXISTS _cdc_{table}_{event}
AFTER {event} ON {table}
BEGIN
    INSERT INTO {log} (table_name, op, pk, before, after)
    VALUES ('{table}', '{op}', {pk}, {before}, {after});
END
"""


def _table_info(conn: sqlite3.Connection, table: str) -> list[tuple]:
    """Columns of a table: (cid, name, type, notnull, default, pk_position)."""
    # nosec B608: identifiers come from sqlite_master, never from user input
    return conn.execute(f"PRAGMA table_info({table})").fetchall()


def _json_row(alias: str, columns: list[str]) -> str:
    """SQL that builds a JSON object of `columns` from the OLD or NEW row."""
    pairs = ", ".join(f"'{col}', {alias}.{col}" for col in columns)
    return f"json_object({pairs})"


def _capture_triggers(conn: sqlite3.Connection, table: str) -> list[str]:
    """Generate the INSERT, UPDATE and DELETE capture triggers for one table."""
    info = _table_info(conn, table)
    columns = [row[1] for row in info]
    key = [row[1] for row in sorted(info, key=lambda row: row[5]) if row[5] > 0]
    if not key:
        raise ValueError(f"{table} has no primary key; its changes cannot be identified")

    specs = [
        ("insert", "I", _json_row("NEW", key), "NULL", _json_row("NEW", columns)),
        (
            "update",
            "U",
            _json_row("NEW", key),
            _json_row("OLD", columns),
            _json_row("NEW", columns),
        ),
        ("delete", "D", _json_row("OLD", key), _json_row("OLD", columns), "NULL"),
    ]
    return [
        _TRIGGER_TEMPLATE.format(
            table=table, event=event, log=CDC_LOG, op=op, pk=pk, before=before, after=after
        )
        for event, op, pk, before, after in specs
    ]


def install_cdc(conn: sqlite3.Connection) -> None:
    """Create the change log and capture triggers on every source table.

    The trigger column lists are fixed at install time. If the source schema
    later changes, the schema guard stops the pipeline before any
    mismatched data is read.
    """
    conn.execute(CDC_LOG_DDL)
    for table in source_tables(conn):
        for ddl in _capture_triggers(conn, table):
            conn.execute(ddl)
    conn.commit()


def read_changes(
    conn: sqlite3.Connection, after_lsn: int = 0, limit: int | None = None
) -> list[ChangeRecord]:
    """Return changes with lsn > after_lsn, in lsn (commit) order."""
    sql = (
        f"SELECT lsn, table_name, op, pk, before, after, captured_at FROM {CDC_LOG} "
        "WHERE lsn > ? ORDER BY lsn"
    )
    params: list = [after_lsn]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    return [
        ChangeRecord(
            lsn=lsn,
            table=table,
            op=op,
            pk=json.loads(pk),
            before=json.loads(before) if before else None,
            after=json.loads(after) if after else None,
            captured_at=captured_at,
        )
        for lsn, table, op, pk, before, after, captured_at in conn.execute(sql, params)
    ]
