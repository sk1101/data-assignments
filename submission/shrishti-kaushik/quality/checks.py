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

# Allowed transfer status transitions (R12), as (from, to); None = newly created.
ALLOWED_TRANSITIONS = [
    (None, "pending"),
    ("pending", "completed"),
    ("pending", "failed"),
    ("completed", "reversed"),
]


def _allowed_transition(from_col: str, to_col: str) -> str:
    """SQL that is true when (from_col → to_col) is an allowed transition."""
    terms = []
    for before, after in ALLOWED_TRANSITIONS:
        left = f"{from_col} IS NULL" if before is None else f"{from_col} = '{before}'"
        terms.append(f"({left} AND {to_col} = '{after}')")
    return " OR ".join(terms)


def business_checks() -> list[Check]:
    return [
        Check(
            "R05",
            "a customer has at most one wallet per currency",
            "SELECT customer_id, currency, COUNT(*) AS wallets FROM curated.wallets "
            "GROUP BY customer_id, currency HAVING COUNT(*) > 1",
        ),
        Check(
            "R06",
            "wallet balance is not negative",
            "SELECT wallet_id, balance_minor FROM curated.wallets WHERE balance_minor < 0",
        ),
        Check(
            "R07",
            "transfer amount is positive",
            "SELECT transfer_id, amount_minor FROM curated.transfers WHERE amount_minor <= 0",
        ),
        Check(
            "R07",
            "ledger amount is positive",
            "SELECT transfer_id, entry_no, amount_minor FROM curated.ledger_entries "
            "WHERE amount_minor <= 0",
        ),
        Check(
            "R08",
            "a transfer moves money between two different wallets",
            "SELECT transfer_id, from_wallet_id, to_wallet_id FROM curated.transfers "
            "WHERE from_wallet_id = to_wallet_id",
        ),
        Check(
            "R09",
            "settlement is not before creation",
            "SELECT transfer_id, created_at, settled_at FROM curated.transfers "
            "WHERE settled_at < created_at",
        ),
        Check(
            "R10",
            "failure reason is present exactly when failed",
            "SELECT transfer_id, status, failure_reason FROM curated.transfers "
            "WHERE (status = 'failed') <> (failure_reason IS NOT NULL)",
        ),
        Check(
            "R11",
            "settlement time is present exactly when completed or reversed",
            "SELECT transfer_id, status, settled_at FROM curated.transfers "
            "WHERE (status IN ('completed', 'reversed')) <> (settled_at IS NOT NULL)",
        ),
        Check(
            "R12",
            "every transfer status change in history is an allowed transition",
            "SELECT transfer_id, previous AS from_status, status AS to_status, _lsn FROM ("
            "  SELECT transfer_id, status, _lsn, "
            "  LAG(status) OVER (PARTITION BY transfer_id ORDER BY _lsn) AS previous "
            "  FROM history.transfers"
            ") WHERE previous IS NOT NULL AND previous <> status "
            f"AND NOT ({_allowed_transition('previous', 'status')})",
        ),
        Check(
            "R12",
            "every status event records an allowed transition",
            "SELECT transfer_id, seq, from_status, to_status "
            "FROM curated.transfer_status_events "
            f"WHERE NOT ({_allowed_transition('from_status', 'to_status')})",
        ),
        Check(
            "R13",
            "ledger entries and status events are never updated or deleted",
            "SELECT 'ledger_entries' AS history_table, _lsn, _op FROM history.ledger_entries "
            "WHERE _op <> 'I' "
            "UNION ALL "
            "SELECT 'transfer_status_events', _lsn, _op FROM history.transfer_status_events "
            "WHERE _op <> 'I'",
        ),
        Check(
            "R14",
            "transfer currency matches both wallets",
            "SELECT t.transfer_id, t.currency, f.currency AS from_currency, "
            "w.currency AS to_currency FROM curated.transfers t "
            "JOIN curated.wallets f ON f.wallet_id = t.from_wallet_id "
            "JOIN curated.wallets w ON w.wallet_id = t.to_wallet_id "
            "WHERE t.currency <> f.currency OR t.currency <> w.currency",
        ),
        Check(
            "R15",
            "every transfer's ledger is balanced",
            "SELECT * FROM ("
            "  SELECT t.transfer_id, t.status, t.amount_minor, "
            "  COALESCE(SUM(l.amount_minor) FILTER (WHERE l.direction = 'debit'), 0) AS debits, "
            "  COALESCE(SUM(l.amount_minor) FILTER (WHERE l.direction = 'credit'), 0) AS credits "
            "  FROM curated.transfers t "
            "  LEFT JOIN curated.ledger_entries l ON l.transfer_id = t.transfer_id "
            "  GROUP BY t.transfer_id, t.status, t.amount_minor"
            ") WHERE debits <> credits "
            "OR (status = 'completed' AND debits <> amount_minor) "
            "OR (status = 'reversed' AND debits <> 2 * amount_minor) "
            "OR (status IN ('pending', 'failed') AND debits <> 0)",
        ),
        Check(
            "R16",
            "only abandoned customers were ever deleted",
            "SELECT customer_id, status, _lsn FROM history.customers "
            "WHERE _is_deleted AND status <> 'abandoned'",
        ),
        Check(
            "R17",
            "every customer has an email or a phone",
            "SELECT customer_id FROM curated.customers WHERE email IS NULL AND phone IS NULL",
        ),
        Check(
            "R17",
            "email is unique",
            "SELECT email, COUNT(*) AS customers FROM curated.customers "
            "WHERE email IS NOT NULL GROUP BY email HAVING COUNT(*) > 1",
        ),
        Check(
            "R17",
            "phone is unique",
            "SELECT phone, COUNT(*) AS customers FROM curated.customers "
            "WHERE phone IS NOT NULL GROUP BY phone HAVING COUNT(*) > 1",
        ),
    ]


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
