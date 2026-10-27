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


def install_cdc(conn: sqlite3.Connection) -> None:
    """Create the change log. Capture triggers are added in step 2.4."""
    conn.execute(CDC_LOG_DDL)
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
