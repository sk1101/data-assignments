"""Schema guard tests: every kind of source schema change is detected, and a
running pipeline stops before ingesting anything (docs/04_schema_contract.md).
"""

import json
import logging

import pytest

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths, ingest_all, ingest_once
from pipeline.lake import lake_files
from pipeline.schema_guard import SchemaContractViolation, check_contract, load_contract
from source.db import SCHEMA_PATH, connect, create_schema
from tests.factories import seed


@pytest.fixture(scope="module")
def contract():
    return load_contract()


def drifted_source(old, new):
    """A fresh source built from schema.sql with one edit applied, as if the
    source team had shipped that change."""
    sql = SCHEMA_PATH.read_text()
    assert old in sql, f"test setup: {old!r} not found in schema.sql"
    conn = connect()
    conn.executescript(sql.replace(old, new, 1))
    return conn


def found(violations):
    return {(v.kind, v.table, v.column) for v in violations}


# ── Detection ────────────────────────────────────────────────────────────────


def test_unchanged_source_matches_contract(contract):
    conn = connect()
    create_schema(conn)
    assert check_contract(conn, contract) == []


DRIFTS = [
    pytest.param(
        "created_at TEXT NOT NULL,",
        "",
        ("missing_column", "customers", "created_at"),
        id="dropped_column",
    ),
    pytest.param(
        "full_name TEXT NOT NULL",
        "name TEXT NOT NULL",
        ("missing_column", "customers", "full_name"),
        id="renamed_column",
    ),
    pytest.param(
        "balance_minor INTEGER NOT NULL",
        "balance_minor TEXT NOT NULL",
        ("type_changed", "wallets", "balance_minor"),
        id="type_changed",
    ),
    pytest.param(
        "full_name TEXT NOT NULL",
        "full_name TEXT",
        ("nullability_changed", "customers", "full_name"),
        id="not_null_dropped",
    ),
    pytest.param(
        "'pending', 'verified', 'rejected'",
        "'pending', 'verified', 'rejected', 'expired'",
        ("enum_changed", "customers", "kyc_status"),
        id="enum_value_added",
    ),
    pytest.param(
        "PRIMARY KEY (transfer_id, entry_no)",
        "PRIMARY KEY (transfer_id, entry_no, wallet_id)",
        ("primary_key_changed", "ledger_entries", None),
        id="primary_key_changed",
    ),
    pytest.param(
        "REFERENCES customers (customer_id)",
        "",
        ("foreign_key_changed", "wallets", None),
        id="foreign_key_removed",
    ),
    pytest.param(
        "balance_minor >= 0",
        "balance_minor >= -100",
        ("ddl_changed", "wallets", None),
        id="check_rule_loosened",
    ),
]


@pytest.mark.parametrize("old, new, expected", DRIFTS)
def test_schema_change_is_detected(contract, old, new, expected):
    conn = drifted_source(old, new)
    assert expected in found(check_contract(conn, contract))


def test_dropped_table_is_detected(contract):
    conn = connect()
    create_schema(conn)
    conn.execute("DROP TABLE transfer_status_events")
    assert ("missing_table", "transfer_status_events", None) in found(
        check_contract(conn, contract)
    )


def test_new_table_is_detected(contract):
    conn = connect()
    create_schema(conn)
    conn.execute("CREATE TABLE refunds (refund_id INTEGER PRIMARY KEY) STRICT")
    assert ("unexpected_table", "refunds", None) in found(check_contract(conn, contract))


# ── Stop the line ────────────────────────────────────────────────────────────


@pytest.fixture
def pipeline(tmp_path):
    conn = connect()
    create_schema(conn)
    install_cdc(conn)
    seed(conn)
    paths = LakePaths.under(tmp_path)
    ingest_all(conn, paths)  # healthy start: everything so far is in the lake
    yield conn, paths
    conn.close()


def test_ingestion_stops_before_writing_anything(pipeline):
    conn, paths = pipeline
    files_before = lake_files(paths.lake_dir)
    checkpoint_before = paths.checkpoint_file.read_text()

    conn.execute("ALTER TABLE customers ADD COLUMN loyalty_tier TEXT")
    conn.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")

    with pytest.raises(SchemaContractViolation) as stopped:
        ingest_once(conn, paths)

    assert ("unexpected_column", "customers", "loyalty_tier") in found(stopped.value.violations)
    assert lake_files(paths.lake_dir) == files_before
    assert paths.checkpoint_file.read_text() == checkpoint_before


def test_alert_file_lists_the_violations(pipeline):
    conn, paths = pipeline
    conn.execute("ALTER TABLE customers RENAME COLUMN full_name TO name")

    with pytest.raises(SchemaContractViolation):
        ingest_once(conn, paths)

    alert = json.loads(paths.alert_file.read_text())
    assert alert["contract_version"] == 1
    assert alert["detected_at"]
    reported = {(v["kind"], v["table"], v["column"]) for v in alert["violations"]}
    assert ("missing_column", "customers", "full_name") in reported
    assert ("unexpected_column", "customers", "name") in reported


def test_stop_is_logged_and_repeats_every_cycle(pipeline, caplog):
    conn, paths = pipeline
    conn.execute("ALTER TABLE customers ADD COLUMN loyalty_tier TEXT")

    with caplog.at_level(logging.ERROR):
        for _ in range(2):
            with pytest.raises(SchemaContractViolation):
                ingest_once(conn, paths)

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 2
    assert "Ingestion stopped" in errors[0].getMessage()
