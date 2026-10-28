#!/usr/bin/env python3
"""Run the CDC lakehouse pipeline.

  python run.py demo                 # the whole story end to end, in data/demo/
  python run.py init                 # create data/source.db with sample activity
  python run.py once                 # one cycle: source → lake → warehouse → checks
  python run.py watch --interval 2   # a cycle every 2 seconds until Ctrl+C

Exit codes: 0 OK, 1 quality checks failed, 2 stopped by the schema guard.
"""

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths
from pipeline.lake import lake_files, read_lake
from pipeline.runner import run_cycle
from pipeline.schema_guard import SchemaContractViolation, load_contract
from pipeline.warehouse import (
    applied_lsn,
    as_of,
    connect_warehouse,
    create_warehouse,
    current,
    restore_point,
)
from source import sample_activity
from source.db import connect, create_schema

DATA = ROOT / "data"
OK, CHECKS_FAILED, SCHEMA_STOP = 0, 1, 2


def init(root: Path) -> None:
    """A fresh source database with CDC installed and sample activity captured."""
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    src = connect(str(root / "source.db"))
    create_schema(src)
    install_cdc(src)
    sample_activity.run_sample_activity(src)
    src.close()


def open_env(root: Path):
    contract = load_contract()
    src = connect(str(root / "source.db"))
    wh = connect_warehouse(root / "warehouse.duckdb")
    create_warehouse(wh, contract)
    return src, wh, LakePaths.under(root), contract


def report_file(paths: LakePaths) -> Path:
    return paths.alert_file.parent / "quality_report.json"


def cycle(root: Path) -> int:
    if not (root / "source.db").exists():
        print("No source database yet. Run: python run.py init")
        return CHECKS_FAILED
    src, wh, paths, contract = open_env(root)
    try:
        result = run_cycle(src, wh, paths, contract, report_file(paths))
    except SchemaContractViolation as stop:
        print(f"STOPPED by the schema guard. {stop}\nAlert written to {paths.alert_file}")
        return SCHEMA_STOP
    finally:
        src.close()
        wh.close()
    print(
        f"Ingested {result.changes_ingested}, applied {result.changes_applied}, "
        f"warehouse at lsn {result.warehouse_lsn}, failed checks: {result.checks_failed}"
    )
    return CHECKS_FAILED if result.checks_failed else OK


def watch(root: Path, interval: float) -> int:
    print(f"Watching: a cycle every {interval}s (Ctrl+C to stop)")
    try:
        while True:
            code = cycle(root)
            if code == SCHEMA_STOP:
                return code
            time.sleep(interval)
    except KeyboardInterrupt:
        return OK


# ── Demo ─────────────────────────────────────────────────────────────────────


def say(title: str) -> None:
    print(f"\n── {title}")


def show_wallets(wh, contract, lsn: int | None = None) -> None:
    rows = current(wh, contract, "wallets") if lsn is None else as_of(wh, contract, "wallets", lsn)
    for wallet in rows:
        amount = wallet["balance_minor"] / 100
        print(f"   wallet {wallet['wallet_id']} ({wallet['currency']}): {amount:,.2f}")


def demo(root: Path = DATA / "demo") -> int:
    init(root)
    src, wh, paths, contract = open_env(root)
    report = report_file(paths)
    try:
        say("1. Sample activity captured by CDC; first cycle builds lake and warehouse")
        result = run_cycle(src, wh, paths, contract, report)
        print(
            f"   ingested {result.changes_ingested} changes, applied {result.changes_applied}, "
            f"failed checks: {result.checks_failed}"
        )
        show_wallets(wh, contract)
        before_new_activity = applied_lsn(wh)

        say("2. New activity: Aisha sends Rohan AED 100.00; one cycle later it is live")
        sample_activity.start_transfer(src, 103, 10, 20, 10_000, sample_activity.at(40))
        sample_activity.complete_transfer(src, 103, sample_activity.at(41))
        started = time.perf_counter()
        result = run_cycle(src, wh, paths, contract, report)
        elapsed = time.perf_counter() - started
        print(f"   {result.changes_applied} changes applied in {elapsed:.2f}s")
        show_wallets(wh, contract)

        say("3. Re-running with nothing new changes nothing")
        result = run_cycle(src, wh, paths, contract, report)
        lsns = [row["lsn"] for row in read_lake(paths.lake_dir)]
        print(
            f"   ingested {result.changes_ingested}, applied {result.changes_applied}; "
            f"lake holds {len(lsns)} changes, {len(set(lsns))} distinct"
        )

        say(f"4. Time travel: wallet balances as of lsn {before_new_activity} (before step 2)")
        show_wallets(wh, contract, lsn=before_new_activity)

        say("5. Restore: Lena's customer record was deleted; recover it from history")
        (deleted_at,) = wh.execute(
            "SELECT _lsn FROM history.customers WHERE customer_id = 3 AND _is_deleted"
        ).fetchone()
        schema = restore_point(wh, contract, deleted_at - 1)
        row = wh.execute(
            f"SELECT customer_id, full_name, email, status FROM {schema}.customers "
            "WHERE customer_id = 3"
        ).fetchone()
        print(f"   {schema}.customers → {row}")

        say("6. The source team adds a column without warning: the pipeline stops")
        files_before = len(lake_files(paths.lake_dir))
        src.execute("ALTER TABLE customers ADD COLUMN loyalty_tier TEXT")
        sample_activity.start_transfer(src, 104, 10, 20, 1_000, sample_activity.at(50))
        try:
            run_cycle(src, wh, paths, contract, report)
        except SchemaContractViolation as stop:
            print(f"   STOPPED: {stop}")
            print(f"   alert: {paths.alert_file}")
            print(f"   lake files before: {files_before}, after: {len(lake_files(paths.lake_dir))}")
        else:
            print("   ✗ expected the pipeline to stop")
            return CHECKS_FAILED
    finally:
        src.close()
        wh.close()

    say(f"Done. Explore the warehouse: duckdb {root / 'warehouse.duckdb'}")
    return OK


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="CDC lakehouse pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("demo", help="run the whole story end to end")
    commands.add_parser("init", help="create data/source.db with sample activity")
    commands.add_parser("once", help="run one pipeline cycle")
    watch_cmd = commands.add_parser("watch", help="run a cycle every N seconds")
    watch_cmd.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args(argv)

    level = logging.WARNING if args.command == "demo" else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(name)s: %(message)s")

    if args.command == "demo":
        return demo()
    if args.command == "init":
        init(DATA)
        print(f"Created {DATA / 'source.db'} with sample activity. Next: python run.py once")
        return OK
    if args.command == "once":
        return cycle(DATA)
    return watch(DATA, args.interval)


if __name__ == "__main__":
    sys.exit(main())
