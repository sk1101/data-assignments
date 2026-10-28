"""Data-quality tests: a clean warehouse passes every check, every rule in
docs/01 has a warehouse check, and each check catches its own broken rule.
"""

import json
import re
from pathlib import Path

import pytest

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths, ingest_all
from pipeline.schema_guard import load_contract
from pipeline.warehouse import apply_changes, connect_warehouse, create_warehouse
from quality.checks import all_checks, format_report, run_checks, write_report
from source.db import connect, create_schema
from tests.factories import seed

SOURCE_DOC = Path(__file__).resolve().parent.parent / "docs" / "01_source_schema.md"


@pytest.fixture(scope="module")
def contract():
    return load_contract()


@pytest.fixture
def env(tmp_path, contract):
    src = connect()
    create_schema(src)
    install_cdc(src)
    seed(src)
    src.execute("DELETE FROM customers WHERE customer_id = 3")  # a tombstone for R16 to check
    paths = LakePaths.under(tmp_path)
    ingest_all(src, paths, contract=contract)
    wh = connect_warehouse()
    create_warehouse(wh, contract)
    apply_changes(wh, contract, paths.lake_dir)
    yield wh, paths
    src.close()
    wh.close()


def failing_rules(results):
    return {result.rule for result in results if not result.passed}


def documented_rules():
    rules = re.findall(r"^\|\s*(R\d{2})\s*\|", SOURCE_DOC.read_text(), re.MULTILINE)
    assert len(rules) >= 17, "could not read the rule table from docs/01_source_schema.md"
    return set(rules)


# ── Coverage ─────────────────────────────────────────────────────────────────


def test_clean_warehouse_passes_every_check(env, contract):
    wh, paths = env
    results = run_checks(wh, contract, paths.lake_dir)
    assert failing_rules(results) == set(), format_report(results)


def test_every_documented_rule_has_a_warehouse_check(contract):
    covered = {check.rule for check in all_checks(contract)}
    missing = sorted(documented_rules() - covered)
    assert not missing, f"rules without a warehouse check: {missing}"


# ── Each check catches its rule ──────────────────────────────────────────────

CORRUPTIONS = [
    pytest.param(
        "R01",
        "INSERT INTO history.customers SELECT * FROM history.customers WHERE customer_id = 1",
        id="R01_duplicate_key",
    ),
    pytest.param(
        "R02", "UPDATE history.wallets SET customer_id = 999 WHERE wallet_id = 20", id="R02"
    ),
    pytest.param(
        "R03", "UPDATE history.customers SET full_name = NULL WHERE customer_id = 1", id="R03"
    ),
    pytest.param(
        "R04",
        "UPDATE history.transfers SET status = 'refunded' WHERE transfer_id = 100",
        id="R04_enum",
    ),
    pytest.param(
        "R04",
        "UPDATE history.customers SET country_code = 'UAE' WHERE customer_id = 1",
        id="R04_country",
    ),
    pytest.param(
        "R05", "UPDATE history.wallets SET customer_id = 1 WHERE wallet_id = 20", id="R05"
    ),
    pytest.param(
        "R06", "UPDATE history.wallets SET balance_minor = -1 WHERE wallet_id = 10", id="R06"
    ),
    pytest.param(
        "R07",
        "UPDATE history.ledger_entries SET amount_minor = 0 "
        "WHERE transfer_id = 200 AND entry_no = 1",
        id="R07",
    ),
    pytest.param(
        "R08", "UPDATE history.transfers SET to_wallet_id = 10 WHERE transfer_id = 100", id="R08"
    ),
    pytest.param(
        "R09",
        "UPDATE history.transfers SET settled_at = '2000-01-01T00:00:00Z' WHERE transfer_id = 200",
        id="R09",
    ),
    pytest.param(
        "R10", "UPDATE history.transfers SET failure_reason = 'x' WHERE transfer_id = 100", id="R10"
    ),
    pytest.param(
        "R11",
        "UPDATE history.transfers SET settled_at = '2026-10-27T11:00:00Z' WHERE transfer_id = 100",
        id="R11",
    ),
    pytest.param(
        "R12",
        "INSERT INTO history.transfer_status_events "
        "(transfer_id, seq, from_status, to_status, changed_at, "
        "_lsn, _op, _is_deleted, _valid_from) "
        "VALUES (200, 3, 'completed', 'pending', '2026-10-27T12:00:00Z', 999, 'I', false, "
        "'2026-10-27T12:00:00Z')",
        id="R12",
    ),
    pytest.param(
        "R13",
        "UPDATE history.ledger_entries SET _op = 'U' WHERE transfer_id = 200 AND entry_no = 1",
        id="R13",
    ),
    pytest.param(
        "R14", "UPDATE history.transfers SET currency = 'USD' WHERE transfer_id = 100", id="R14"
    ),
    pytest.param(
        "R15",
        "UPDATE history.ledger_entries SET amount_minor = 400 "
        "WHERE transfer_id = 200 AND entry_no = 2",
        id="R15",
    ),
    pytest.param(
        "R16", "UPDATE history.customers SET status = 'active' WHERE customer_id = 3", id="R16"
    ),
    pytest.param(
        "R17",
        "UPDATE history.customers SET email = NULL, phone = NULL WHERE customer_id = 1",
        id="R17_no_identifier",
    ),
    pytest.param(
        "R17",
        "UPDATE history.customers SET email = 'a@example.com' WHERE customer_id = 2",
        id="R17_duplicate_email",
    ),
    pytest.param("C01", "UPDATE meta.applied SET last_lsn = last_lsn - 1", id="C01_behind"),
]


@pytest.mark.parametrize("rule, corruption", CORRUPTIONS)
def test_check_catches_its_broken_rule(env, contract, rule, corruption):
    wh, paths = env
    wh.execute(corruption)
    assert rule in failing_rules(run_checks(wh, contract, paths.lake_dir))


# ── Reporting ────────────────────────────────────────────────────────────────


def test_failures_report_the_offending_rows(env, contract, tmp_path):
    wh, paths = env
    wh.execute("UPDATE history.wallets SET balance_minor = -1 WHERE wallet_id = 10")
    results = run_checks(wh, contract, paths.lake_dir)

    failed = [result for result in results if not result.passed]
    assert [result.rule for result in failed] == ["R06"]
    assert failed[0].sample[0]["wallet_id"] == 10
    assert "✗ R06" in format_report(results)

    report_path = tmp_path / "quality_report.json"
    write_report(results, report_path)
    saved = json.loads(report_path.read_text())
    assert any(r["rule"] == "R06" and not r["passed"] for r in saved["results"])
