#!/usr/bin/env python3
"""CI check for the schema contract (docs/04_schema_contract.md).

1. source/schema.sql matches the committed contract.
2. The guard is not vacuous: known breaking changes are each detected.

Exit 0 when both hold, 1 otherwise.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.schema_guard import check_contract, load_contract
from source.db import SCHEMA_PATH, connect, create_schema

SELF_TEST_DRIFTS = {
    "column made nullable": ("full_name TEXT NOT NULL", "full_name TEXT"),
    "column type changed": ("balance_minor INTEGER NOT NULL", "balance_minor TEXT NOT NULL"),
    "allowed value added": (
        "'pending', 'verified', 'rejected'",
        "'pending', 'verified', 'rejected', 'expired'",
    ),
    "check rule loosened": ("balance_minor >= 0", "balance_minor >= -100"),
}


def main() -> int:
    contract = load_contract()
    failures: list[str] = []

    conn = connect()
    create_schema(conn)
    violations = check_contract(conn, contract)
    if violations:
        failures.append(f"source/schema.sql does not match contract v{contract['version']}:")
        failures += [f"    {v.describe()}" for v in violations]
    else:
        tables = len(contract["tables"])
        print(f"✓ source schema matches contract v{contract['version']} ({tables} tables)")

    sql = SCHEMA_PATH.read_text()
    for name, (old, new) in SELF_TEST_DRIFTS.items():
        if old not in sql:
            failures.append(f"self-test setup: {old!r} not found in schema.sql")
            continue
        drifted = connect()
        drifted.executescript(sql.replace(old, new, 1))
        if check_contract(drifted, contract):
            print(f"✓ guard detects: {name}")
        else:
            failures.append(f"guard missed: {name}")

    if failures:
        print("✗ Schema contract check FAILED:")
        print("\n".join(f"  {line}" for line in failures))
        return 1
    print("All schema contract checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())