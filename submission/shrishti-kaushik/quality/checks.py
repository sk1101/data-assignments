"""Warehouse data-quality checks, one or more per source rule.
See docs/06_validation_parity.md.

Each check is a SQL query that returns the rows BREAKING a rule; zero rows = pass.
"""

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

import duckdb

from pipeline.lake import highest_lsn_in_lake
from pipeline.timeutil import utc_now
from pipeline.warehouse import applied_lsn

SAMPLE_SIZE = 5


@dataclass(frozen=True)
class Check:
    rule: str
    name: str
    sql: str


@dataclass(frozen=True)
class CheckResult:
    rule: str
    name: str
    failing_rows: int
    sample: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


# ── System rules R01-R04: generated from the contract ────────────────────────


def system_checks(contract: dict) -> list[Check]:
    checks: list[Check] = []
    for table, spec in contract["tables"].items():
        key = ", ".join(spec["primary_key"])
        checks.append(
            Check(
                "R01",
                f"{table}: primary key is unique",
                f"SELECT {key}, COUNT(*) AS copies FROM curated.{table} "
                f"GROUP BY {key} HAVING COUNT(*) > 1",
            )
        )

        for fk in spec["foreign_keys"]:
            pairs = zip(fk["columns"], fk["ref_columns"], strict=True)
            match = " AND ".join(f"p.{ref} = c.{col}" for col, ref in pairs)
            present = " AND ".join(f"c.{col} IS NOT NULL" for col in fk["columns"])
            checks.append(
                Check(
                    "R02",
                    f"{table}.{', '.join(fk['columns'])} references {fk['references']}",
                    f"SELECT c.* FROM curated.{table} c WHERE {present} AND NOT EXISTS "
                    f"(SELECT 1 FROM curated.{fk['references']} p WHERE {match})",
                )
            )

        required = [col for col, d in spec["columns"].items() if not d["nullable"]]
        if required:
            checks.append(
                Check(
                    "R03",
                    f"{table}: required columns are present",
                    f"SELECT * FROM curated.{table} WHERE "
                    + " OR ".join(f"{col} IS NULL" for col in required),
                )
            )

        for column, values in spec["enums"].items():
            allowed = ", ".join(f"'{value}'" for value in values)
            checks.append(
                Check(
                    "R04",
                    f"{table}.{column} is an allowed value",
                    f"SELECT * FROM curated.{table} "
                    f"WHERE {column} IS NOT NULL AND {column} NOT IN ({allowed})",
                )
            )

    checks.append(
        Check(
            "R04",
            "customers.country_code is two capital letters",
            "SELECT customer_id, country_code FROM curated.customers "
            "WHERE NOT regexp_full_match(country_code, '[A-Z]{2}')",
        )
    )
    return checks


# ── Business rules R05-R17 ───────────────────────────────────────────────────


def business_checks() -> list[Check]:
    # GAP (first version): business rules are not checked in the warehouse yet.
    return []


def all_checks(contract: dict) -> list[Check]:
    return system_checks(contract) + business_checks()


# ── Running and reporting ────────────────────────────────────────────────────


def run_checks(
    conn: duckdb.DuckDBPyConnection, contract: dict, lake_dir: Path | None = None
) -> list[CheckResult]:
    """Run every check; with lake_dir, also check completeness against the lake (C01)."""
    results: list[CheckResult] = []
    for check in all_checks(contract):
        cursor = conn.execute(check.sql)
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        sample = [dict(zip(columns, row, strict=True)) for row in rows[:SAMPLE_SIZE]]
        results.append(CheckResult(check.rule, check.name, len(rows), sample))

    if lake_dir is not None:
        lake_lsn, warehouse_lsn = highest_lsn_in_lake(lake_dir), applied_lsn(conn)
        behind = (
            []
            if lake_lsn == warehouse_lsn
            else [{"lake_lsn": lake_lsn, "warehouse_lsn": warehouse_lsn}]
        )
        results.append(
            CheckResult(
                "C01", "warehouse has applied every change in the lake", len(behind), behind
            )
        )
    return results


def format_report(results: list[CheckResult]) -> str:
    lines = []
    for result in results:
        if result.passed:
            lines.append(f"✓ {result.rule} {result.name}")
        else:
            lines.append(
                f"✗ {result.rule} {result.name}: {result.failing_rows} failing row(s), "
                f"e.g. {result.sample[0]}"
            )
    failed = sum(not result.passed for result in results)
    lines.append(f"{len(results) - failed}/{len(results)} checks passed")
    return "\n".join(lines)


def write_report(results: list[CheckResult], path: Path) -> None:
    report = {
        "generated_at": utc_now(),
        "results": [asdict(result) | {"passed": result.passed} for result in results],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str) + "\n")
