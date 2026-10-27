"""Connection helper for the source database (the system of record)."""

import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"


def connect(path: str = ":memory:") -> sqlite3.Connection:
    """Open the source database with foreign keys enforced (R02)."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    """Create the source tables, constraints, indexes and triggers."""
    conn.executescript(SCHEMA_PATH.read_text())
