"""CDC capture tests: every committed change, and only committed changes,
lands in _cdc_log once, in order, with full before/after images.
"""

import sqlite3

import pytest

from pipeline.capture import install_cdc, read_changes, source_tables
from source.db import connect, create_schema
from tests.factories import customer, insert, ledger, seed


@pytest.fixture
def db():
    """Source with CDC installed BEFORE seeding, so the seed data is captured too."""
    conn = connect()
    create_schema(conn)
    install_cdc(conn)
    seed(conn)
    yield conn
    conn.close()


def latest_lsn(conn):
    return conn.execute("SELECT COALESCE(MAX(lsn), 0) FROM _cdc_log").fetchone()[0]


def columns_of(conn, table):
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


# ── Complete: every table, every operation ───────────────────────────────────


def test_every_source_table_is_captured(db):
    captured = {change.table for change in read_changes(db)}
    assert captured == set(source_tables(db))


def test_insert_is_captured_with_full_after_image(db):
    mark = latest_lsn(db)
    insert(db, "customers", **customer(customer_id=50, email="new@example.com"))
    changes = read_changes(db, after_lsn=mark)
    assert len(changes) == 1
    change = changes[0]
    assert (change.table, change.op, change.pk) == ("customers", "I", {"customer_id": 50})
    assert change.before is None
    assert set(change.after) == columns_of(db, "customers")
    assert change.after["email"] == "new@example.com"
    assert change.after["phone"] is None


def test_update_is_captured_with_before_and_after(db):
    mark = latest_lsn(db)
    db.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    changes = read_changes(db, after_lsn=mark)
    assert len(changes) == 1
    change = changes[0]
    assert (change.table, change.op, change.pk) == ("wallets", "U", {"wallet_id": 10})
    assert change.before["balance_minor"] == 5000
    assert change.after["balance_minor"] == 4000


def test_delete_is_captured_with_deleted_row(db):
    mark = latest_lsn(db)
    db.execute("DELETE FROM customers WHERE customer_id = 3")
    changes = read_changes(db, after_lsn=mark)
    assert len(changes) == 1
    change = changes[0]
    assert (change.table, change.op, change.pk) == ("customers", "D", {"customer_id": 3})
    assert change.before["phone"] == "+971500000003"
    assert change.after is None


def test_composite_primary_key_is_captured(db):
    mark = latest_lsn(db)
    insert(db, "ledger_entries", **ledger(entry_no=3))
    changes = read_changes(db, after_lsn=mark)
    assert len(changes) == 1
    assert changes[0].pk == {"transfer_id": 200, "entry_no": 3}


def test_multi_row_update_produces_one_record_per_row(db):
    mark = latest_lsn(db)
    db.execute("UPDATE wallets SET status = 'frozen'")
    changes = read_changes(db, after_lsn=mark)
    assert sorted(change.pk["wallet_id"] for change in changes) == [10, 20]


def test_values_keep_their_types(db):
    wallet_changes = [c for c in read_changes(db) if c.table == "wallets"]
    assert wallet_changes, "no wallet changes captured"
    after = wallet_changes[0].after
    assert isinstance(after["balance_minor"], int)
    assert isinstance(after["currency"], str)


# ── Atomic: only committed changes ───────────────────────────────────────────


def test_rejected_write_is_not_captured(db):
    mark = latest_lsn(db)
    db.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    with pytest.raises(sqlite3.IntegrityError):
        db.execute("UPDATE wallets SET balance_minor = -1 WHERE wallet_id = 10")
    changes = read_changes(db, after_lsn=mark)
    assert [change.after["balance_minor"] for change in changes] == [4000]


def test_rolled_back_transaction_is_not_captured(db):
    mark = latest_lsn(db)
    db.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    db.commit()
    db.execute("UPDATE wallets SET balance_minor = 3000 WHERE wallet_id = 10")
    db.rollback()
    changes = read_changes(db, after_lsn=mark)
    assert [change.after["balance_minor"] for change in changes] == [4000]


# ── Ordered, stable identity, resumable ──────────────────────────────────────


def test_changes_are_returned_in_commit_order(db):
    mark = latest_lsn(db)
    db.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    db.execute("UPDATE wallets SET balance_minor = 1000 WHERE wallet_id = 20")
    db.execute("UPDATE wallets SET balance_minor = 3000 WHERE wallet_id = 10")
    changes = read_changes(db, after_lsn=mark)
    assert [change.after["balance_minor"] for change in changes] == [4000, 1000, 3000]
    lsns = [change.lsn for change in changes]
    assert lsns == sorted(lsns)
    assert len(set(lsns)) == len(lsns)


def test_lsn_is_never_reused_after_pruning(db):
    highest_before_prune = latest_lsn(db)
    db.execute("DELETE FROM _cdc_log")
    db.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    changes = read_changes(db)
    assert len(changes) == 1
    assert changes[0].lsn > highest_before_prune


def test_reading_resumes_after_a_checkpoint(db):
    all_changes = read_changes(db)
    assert len(all_changes) > 5, "seed data was not captured"
    checkpoint = all_changes[2].lsn
    assert read_changes(db, after_lsn=checkpoint) == all_changes[3:]
    assert read_changes(db, after_lsn=checkpoint, limit=2) == all_changes[3:5]
