"""Warehouse tests: the current snapshot equals the source, every past state can
be reconstructed, and replays or crashes never corrupt history (docs/05_warehouse.md).
"""

import pytest

from pipeline import warehouse
from pipeline.capture import install_cdc, read_changes
from pipeline.ingest import LakePaths, ingest_all
from pipeline.lake import read_lake, write_batch
from pipeline.schema_guard import load_contract
from pipeline.warehouse import (
    applied_lsn,
    apply_changes,
    as_of,
    connect_warehouse,
    create_warehouse,
    current,
    lsn_at,
    rebuild,
    restore_point,
)
from source.db import connect, create_schema
from tests.factories import seed


@pytest.fixture(scope="module")
def contract():
    return load_contract()


@pytest.fixture
def env(tmp_path, contract):
    src = connect()
    create_schema(src)
    install_cdc(src)
    seed(src)
    paths = LakePaths.under(tmp_path)
    wh = connect_warehouse()
    create_warehouse(wh, contract)
    yield src, paths, wh
    src.close()
    wh.close()


# ── helpers ──────────────────────────────────────────────────────────────────


def sync(src, paths, wh, contract):
    """One full pipeline pass: source → lake → warehouse."""
    ingest_all(src, paths, contract=contract)
    return apply_changes(wh, contract, paths.lake_dir)


def source_rows(src, contract, table):
    spec = contract["tables"][table]
    cols = list(spec["columns"])
    rows = src.execute(
        f"SELECT {', '.join(cols)} FROM {table} ORDER BY {', '.join(spec['primary_key'])}"
    ).fetchall()
    return [dict(zip(cols, row, strict=True)) for row in rows]


def source_state(src, contract):
    return {table: source_rows(src, contract, table) for table in contract["tables"]}


def warehouse_state(wh, contract):
    return {table: current(wh, contract, table) for table in contract["tables"]}


def warehouse_state_as_of(wh, contract, lsn):
    return {table: as_of(wh, contract, table, lsn) for table in contract["tables"]}


def latest_lsn(src):
    return src.execute("SELECT COALESCE(MAX(lsn), 0) FROM _cdc_log").fetchone()[0]


def history_counts(wh, contract):
    """Per table: (number of versions, number of distinct lsns)."""
    return {
        table: wh.execute(f"SELECT COUNT(*), COUNT(DISTINCT _lsn) FROM history.{table}").fetchone()
        for table in contract["tables"]
    }


# ── Current snapshot ─────────────────────────────────────────────────────────


def test_current_state_matches_source(env, contract):
    src, paths, wh = env
    sync(src, paths, wh, contract)
    assert warehouse_state(wh, contract) == source_state(src, contract)


def test_updates_and_deletes_are_reflected(env, contract):
    src, paths, wh = env
    sync(src, paths, wh, contract)
    src.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    src.execute(
        "UPDATE transfers SET status = 'failed', failure_reason = 'insufficient funds' "
        "WHERE transfer_id = 100"
    )
    src.execute("DELETE FROM customers WHERE customer_id = 3")
    sync(src, paths, wh, contract)
    assert warehouse_state(wh, contract) == source_state(src, contract)
    assert applied_lsn(wh) == latest_lsn(src)


def test_history_keeps_every_version(env, contract):
    src, paths, wh = env
    src.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    src.execute("UPDATE wallets SET balance_minor = 3000 WHERE wallet_id = 10")
    sync(src, paths, wh, contract)
    versions = wh.execute(
        "SELECT balance_minor, _valid_to_lsn IS NULL FROM history.wallets "
        "WHERE wallet_id = 10 ORDER BY _lsn"
    ).fetchall()
    assert versions == [(5000, False), (4000, False), (3000, True)]


def test_delete_keeps_a_tombstone_with_the_deleted_values(env, contract):
    src, paths, wh = env
    src.execute("DELETE FROM customers WHERE customer_id = 3")
    sync(src, paths, wh, contract)
    versions = wh.execute(
        "SELECT _op, _is_deleted, phone FROM history.customers "
        "WHERE customer_id = 3 ORDER BY _lsn"
    ).fetchall()
    assert versions == [("I", False, "+971500000003"), ("D", True, "+971500000003")]


# ── Time travel and restore ──────────────────────────────────────────────────


def test_as_of_reconstructs_every_past_state(env, contract):
    src, paths, wh = env
    recorded = [(latest_lsn(src), source_state(src, contract))]
    for statement in [
        "UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10",
        "UPDATE transfers SET status = 'failed', failure_reason = 'x' WHERE transfer_id = 100",
        "UPDATE customers SET kyc_status = 'rejected' WHERE customer_id = 2",
        "DELETE FROM customers WHERE customer_id = 3",
    ]:
        src.execute(statement)
        recorded.append((latest_lsn(src), source_state(src, contract)))

    sync(src, paths, wh, contract)
    for lsn, expected in recorded:
        assert warehouse_state_as_of(wh, contract, lsn) == expected, f"state at lsn {lsn}"


def test_time_is_translated_to_the_last_lsn_at_or_before_it(env, contract):
    src, paths, wh = env
    sync(src, paths, wh, contract)
    last = read_changes(src)[-1]
    assert lsn_at(wh, contract, last.captured_at) == last.lsn
    assert lsn_at(wh, contract, "2000-01-01T00:00:00.000Z") == 0


def test_restore_point_recovers_a_mistaken_delete(env, contract):
    src, paths, wh = env
    before_mistake = latest_lsn(src)
    src.execute("DELETE FROM customers WHERE customer_id = 3")  # the mistake
    sync(src, paths, wh, contract)
    assert 3 not in [c["customer_id"] for c in current(wh, contract, "customers")]

    schema = restore_point(wh, contract, before_mistake)
    restored = wh.execute(
        f"SELECT customer_id, phone FROM {schema}.customers WHERE customer_id = 3"
    ).fetchall()
    assert restored == [(3, "+971500000003")]


def test_rebuild_from_lake_reproduces_any_point(env, contract):
    src, paths, wh = env
    middle_lsn, middle_state = latest_lsn(src), source_state(src, contract)
    src.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    src.execute("DELETE FROM customers WHERE customer_id = 3")
    sync(src, paths, wh, contract)
    final_state = warehouse_state(wh, contract)

    rebuild(wh, contract, paths.lake_dir, up_to_lsn=middle_lsn)
    assert warehouse_state(wh, contract) == middle_state

    rebuild(wh, contract, paths.lake_dir)
    assert warehouse_state(wh, contract) == final_state


# ── Reliability ──────────────────────────────────────────────────────────────


def test_reapplying_is_idempotent(env, contract):
    src, paths, wh = env
    sync(src, paths, wh, contract)
    counts = history_counts(wh, contract)
    assert apply_changes(wh, contract, paths.lake_dir) == 0
    assert history_counts(wh, contract) == counts


def test_crash_mid_apply_leaves_no_partial_batch(env, contract, monkeypatch):
    src, paths, wh = env
    ingest_all(src, paths, contract=contract)

    real_apply = warehouse._apply_change
    calls = {"n": 0}

    def crash_on_fifth(conn, contract_, change):
        calls["n"] += 1
        if calls["n"] == 5:
            raise RuntimeError("simulated crash mid-apply")
        real_apply(conn, contract_, change)

    monkeypatch.setattr(warehouse, "_apply_change", crash_on_fifth)
    with pytest.raises(RuntimeError):
        apply_changes(wh, contract, paths.lake_dir)

    assert applied_lsn(wh) == 0
    assert all(
        total == 0 for total, _ in history_counts(wh, contract).values()
    ), "part of the batch was applied"

    monkeypatch.undo()
    apply_changes(wh, contract, paths.lake_dir)
    assert warehouse_state(wh, contract) == source_state(src, contract)
    assert all(total == distinct for total, distinct in history_counts(wh, contract).values())


def test_duplicate_lake_records_are_applied_once(env, contract):
    src, paths, wh = env
    ingest_all(src, paths, contract=contract)
    # e.g. an operator re-copies part of a batch during a backfill
    write_batch(paths.lake_dir, read_changes(src)[3:6])
    assert len(read_lake(paths.lake_dir)) > len(read_changes(src)), "duplicate not set up"

    apply_changes(wh, contract, paths.lake_dir)
    assert warehouse_state(wh, contract) == source_state(src, contract)
    assert all(
        total == distinct for total, distinct in history_counts(wh, contract).values()
    ), "a change was recorded as more than one version"
