"""Schema guard: compares the live source schema against the committed contract
and stops ingestion on any difference (fail closed). See docs/04_schema_contract.md.
"""

import hashlib
import json
import logging
import re
import sqlite3
from dataclasses import asdict, dataclass
from pathlib import Path

from pipeline.capture import source_tables
from pipeline.timeutil import utc_now

log = logging.getLogger(__name__)

CONTRACT_PATH = Path(__file__).resolve().parent.parent / "contracts" / "source_contract.json"

# Matches rules shaped exactly like: CHECK (column IN ('a', 'b', ...))
_ENUM_CHECK = re.compile(r"CHECK\s*\(\s*(\w+)\s+IN\s*\(([^)]*)\)\s*\)", re.IGNORECASE)


# ── Describing the live schema ───────────────────────────────────────────────


def _normalise_ddl(sql: str) -> str:
    """Lowercase and collapse whitespace, so re-indenting does not change the fingerprint."""
    text = re.sub(r"\s+", " ", sql.strip().lower())
    return re.sub(r"\s*([(),])\s*", r"\1", text)


def ddl_fingerprint(sql: str) -> str:
    return hashlib.sha256(_normalise_ddl(sql).encode()).hexdigest()


def describe_table(conn: sqlite3.Connection, table: str) -> dict:
    """Columns, keys, allowed values and DDL fingerprint of one table, from SQLite's catalogue."""
    info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = {
        name: {"type": col_type.upper(), "nullable": not notnull and pk == 0}
        for _cid, name, col_type, notnull, _default, pk in info
    }
    primary_key = [row[1] for row in sorted(info, key=lambda row: row[5]) if row[5] > 0]

    grouped: dict[int, dict] = {}
    for fk_id, _seq, ref_table, from_col, to_col, *_rest in conn.execute(
        f"PRAGMA foreign_key_list({table})"
    ):
        fk = grouped.setdefault(fk_id, {"columns": [], "references": ref_table, "ref_columns": []})
        fk["columns"].append(from_col)
        fk["ref_columns"].append(to_col)
    foreign_keys = sorted(grouped.values(), key=lambda fk: (fk["columns"], fk["references"]))

    (sql,) = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    enums = {
        column: sorted(value.strip().strip("'") for value in values.split(","))
        for column, values in _ENUM_CHECK.findall(sql)
    }

    return {
        "columns": columns,
        "primary_key": primary_key,
        "foreign_keys": foreign_keys,
        "enums": enums,
        "ddl_sha256": ddl_fingerprint(sql),
    }


def describe_source(conn: sqlite3.Connection) -> dict:
    """The live schema of every captured source table."""
    return {table: describe_table(conn, table) for table in source_tables(conn)}


# ── The contract file ────────────────────────────────────────────────────────


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    return json.loads(path.read_text())


def write_contract(path: Path, tables: dict, version: int) -> None:
    contract = {"version": version, "generated_from": "source/schema.sql", "tables": tables}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n")


# ── Comparing ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Violation:
    kind: str
    table: str
    column: str | None = None
    expected: object = None
    actual: object = None

    def describe(self) -> str:
        where = f"{self.table}.{self.column}" if self.column else self.table
        details = []
        if self.expected is not None:
            details.append(f"expected {self.expected!r}")
        if self.actual is not None:
            details.append(f"found {self.actual!r}")
        return f"{self.kind}: {where}" + (f" ({', '.join(details)})" if details else "")


class SchemaContractViolation(RuntimeError):
    """Raised to stop ingestion when the live schema differs from the contract."""

    def __init__(self, violations: list[Violation]):
        self.violations = violations
        lines = "\n".join(f"  - {v.describe()}" for v in violations)
        super().__init__(f"source schema does not match the contract:\n{lines}")


def _compare_table(table: str, expected: dict, actual: dict) -> list[Violation]:
    found: list[Violation] = []
    exp_cols, act_cols = expected["columns"], actual["columns"]

    for column in sorted(exp_cols.keys() - act_cols.keys()):
        found.append(Violation("missing_column", table, column, exp_cols[column], None))
    for column in sorted(act_cols.keys() - exp_cols.keys()):
        found.append(Violation("unexpected_column", table, column, None, act_cols[column]))
    for column in sorted(exp_cols.keys() & act_cols.keys()):
        exp, act = exp_cols[column], act_cols[column]
        if exp["type"] != act["type"]:
            found.append(Violation("type_changed", table, column, exp["type"], act["type"]))
        if exp["nullable"] != act["nullable"]:
            found.append(
                Violation("nullability_changed", table, column, exp["nullable"], act["nullable"])
            )

    for column in sorted(expected["enums"].keys() | actual["enums"].keys()):
        exp, act = expected["enums"].get(column), actual["enums"].get(column)
        if exp != act:
            found.append(Violation("enum_changed", table, column, exp, act))

    if expected["primary_key"] != actual["primary_key"]:
        found.append(
            Violation(
                "primary_key_changed", table, None, expected["primary_key"], actual["primary_key"]
            )
        )
    if expected["foreign_keys"] != actual["foreign_keys"]:
        found.append(
            Violation(
                "foreign_key_changed", table, None, expected["foreign_keys"], actual["foreign_keys"]
            )
        )

    # Safety net: the definition changed in a way none of the checks above explain.
    if not found and expected["ddl_sha256"] != actual["ddl_sha256"]:
        found.append(
            Violation(
                "ddl_changed", table, None, expected["ddl_sha256"][:12], actual["ddl_sha256"][:12]
            )
        )
    return found


def check_contract(conn: sqlite3.Connection, contract: dict) -> list[Violation]:
    """Every difference between the live schema and the contract (empty list = match)."""
    live = describe_source(conn)
    expected = contract["tables"]
    violations: list[Violation] = []

    for table in sorted(expected.keys() - live.keys()):
        violations.append(Violation("missing_table", table))
    for table in sorted(live.keys() - expected.keys()):
        violations.append(Violation("unexpected_table", table))
    for table in sorted(expected.keys() & live.keys()):
        violations.extend(_compare_table(table, expected[table], live[table]))

    return violations


def enforce_contract(conn: sqlite3.Connection, contract: dict, alert_file: Path) -> None:
    """Stop the line: on any violation, write an alert, log an error and raise."""
    violations = check_contract(conn, contract)
    if not violations:
        return
    alert = {
        "detected_at": utc_now(),
        "contract_version": contract["version"],
        "violations": [asdict(v) for v in violations],
    }
    alert_file.parent.mkdir(parents=True, exist_ok=True)
    alert_file.write_text(json.dumps(alert, indent=2) + "\n")
    error = SchemaContractViolation(violations)
    log.error("Ingestion stopped. %s. Alert written to %s", error, alert_file)
    raise error
