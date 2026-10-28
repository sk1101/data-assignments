#!/usr/bin/env python3
"""CI data-quality check (docs/06_validation_parity.md).

Runs the whole pipeline on sample activity (source → lake → warehouse), then:
1. every warehouse check must pass, and
2. a deliberately broken rule must be caught, so the checks are not vacuous.

Exit 0 when both hold, 1 otherwise.
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths, ingest_all
from pipeline.schema_guard import load_contract
from pipeline.warehouse import apply_changes, connect_warehouse, create_warehouse
from quality.checks import format_report, run_checks
from source.db import connect, create_schema
from source.sample_activity import run_sample_activity


def main() -> int:
    contract = load_contract()
    with tempfile.TemporaryDirectory() as tmp:
        src = connect()
        create_schema(src)
        install_cdc(src)
        run_sample_activity(src)

        paths = LakePaths.under(Path(tmp))
        ingest_all(src, paths, contract=contract)
        wh = connect_warehouse()
        create_warehouse(wh, contract)
        apply_changes(wh, contract, paths.lake_dir)

        results = run_checks(wh, contract, paths.lake_dir)
        print(format_report(results))
        all_passed = all(result.passed for result in results)

        # Self-test: a known violation must be caught, or the checks prove nothing.
        wh.execute("UPDATE history.wallets SET balance_minor = -1 WHERE wallet_id = 10")
        caught = any(r.rule == "R06" and not r.passed for r in run_checks(wh, contract))
        print("✓ self-test: a negative balance is caught" if caught else "✗ self-test FAILED")

    return 0 if all_passed and caught else 1


if __name__ == "__main__":
    sys.exit(main())
