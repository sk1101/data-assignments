"""Source constraint tests: the source must reject every invalid state in docs/01.

Each test names the rule it checks (R01-R17) and asserts the *reason* for the
rejection, so a test cannot pass because some other rule happened to fire.
R14 and R15 span several rows/tables and are enforced by the application,
so they are asserted in the warehouse rather than here.
"""

import sqlite3

import pytest

from pipeline.schema_guard import load_contract
from source.db import connect, create_schema
from tests.factories import EARLIER, LATER, customer, event, insert, ledger, seed, transfer, wallet


def rejected(reason):
    """The write inside this block must be refused, with `reason` in the error."""
    return pytest.raises(sqlite3.IntegrityError, match=reason)


# A valid NEW row per table that does not clash with the fixture data.
NEW_ROW = {
    "customers": lambda: customer(customer_id=50, email="new@example.com"),
    "wallets": lambda: wallet(wallet_id=50, customer_id=3),
    "transfers": lambda: transfer(transfer_id=500),
    "ledger_entries": lambda: ledger(),
    "transfer_status_events": lambda: event(transfer_id=200, seq=3),
}


@pytest.fixture
def db():
    conn = connect()
    create_schema(conn)
    seed(conn)
    yield conn
    conn.close()


# ── R01 primary keys: every table ────────────────────────────────────────────

DUPLICATE_KEYS = [
    pytest.param("customers", customer(customer_id=1, email="x@example.com"), id="customers"),
    pytest.param("wallets", wallet(wallet_id=10, customer_id=3), id="wallets"),
    pytest.param("transfers", transfer(transfer_id=100), id="transfers"),
    pytest.param("ledger_entries", ledger(entry_no=1), id="ledger_entries"),
    pytest.param("transfer_status_events", event(transfer_id=200, seq=1), id="status_events"),
]


@pytest.mark.parametrize("table, row", DUPLICATE_KEYS)
def test_r01_duplicate_primary_key_is_rejected(db, table, row):
    with rejected("UNIQUE constraint failed"):
        insert(db, table, **row)


# ── R02 foreign keys: every relationship ─────────────────────────────────────

MISSING_PARENTS = [
    pytest.param("wallets", wallet(wallet_id=50, customer_id=999), id="wallets.customer_id"),
    pytest.param(
        "transfers", transfer(transfer_id=500, from_wallet_id=999), id="transfers.from_wallet_id"
    ),
    pytest.param(
        "transfers", transfer(transfer_id=500, to_wallet_id=999), id="transfers.to_wallet_id"
    ),
    pytest.param("ledger_entries", ledger(transfer_id=999), id="ledger_entries.transfer_id"),
    pytest.param("ledger_entries", ledger(wallet_id=999), id="ledger_entries.wallet_id"),
    pytest.param("transfer_status_events", event(transfer_id=999), id="status_events.transfer_id"),
]


@pytest.mark.parametrize("table, row", MISSING_PARENTS)
def test_r02_reference_to_missing_parent_is_rejected(db, table, row):
    with rejected("FOREIGN KEY constraint failed"):
        insert(db, table, **row)


# ── R03 required columns: every one ──────────────────────────────────────────

CONTRACT = load_contract()


def _is_rowid_alias(table, column):
    """An INTEGER PRIMARY KEY is auto-assigned by SQLite when NULL, so it can't be null-tested."""
    spec = CONTRACT["tables"][table]
    return spec["primary_key"] == [column] and spec["columns"][column]["type"] == "INTEGER"


NOT_NULL_CASES = [
    pytest.param(table, column, id=f"{table}.{column}")
    for table, spec in CONTRACT["tables"].items()
    for column, definition in spec["columns"].items()
    if not definition["nullable"] and not _is_rowid_alias(table, column)
]


@pytest.mark.parametrize("table, column", NOT_NULL_CASES)
def test_r03_required_column_cannot_be_null(db, table, column):
    row = NEW_ROW[table]()
    row[column] = None
    with rejected("NOT NULL constraint failed"):
        insert(db, table, **row)


# ── R04 allowed values and types ─────────────────────────────────────────────

BAD_VALUES = [
    pytest.param(table, column, "not_an_allowed_value", id=f"{table}.{column}")
    for table, spec in CONTRACT["tables"].items()
    for column in spec["enums"]
] + [
    # Not an allowed-values rule (a GLOB pattern), so listed explicitly.
    pytest.param("customers", "country_code", "UAE", id="customers.country_code"),
]


@pytest.mark.parametrize("table, column, value", BAD_VALUES)
def test_r04_value_outside_allowed_set_is_rejected(db, table, column, value):
    row = NEW_ROW[table]()
    row[column] = value
    with rejected("CHECK constraint failed"):
        insert(db, table, **row)


WRONG_TYPES = [
    pytest.param("transfers", "amount_minor", "ten", id="transfers.amount_minor"),
    pytest.param("wallets", "balance_minor", "lots", id="wallets.balance_minor"),
    pytest.param("ledger_entries", "amount_minor", "ten", id="ledger_entries.amount_minor"),
]


@pytest.mark.parametrize("table, column, value", WRONG_TYPES)
def test_r04_wrong_type_is_rejected(db, table, column, value):
    row = NEW_ROW[table]()
    row[column] = value
    with rejected("cannot store"):
        insert(db, table, **row)


# ── R05 one wallet per customer per currency ─────────────────────────────────


def test_r05_second_wallet_in_same_currency_is_rejected(db):
    insert(db, "wallets", **wallet(wallet_id=30, customer_id=3, currency="USD"))
    with rejected("UNIQUE constraint failed"):
        insert(db, "wallets", **wallet(wallet_id=31, customer_id=3, currency="USD"))


# ── R06–R08 amounts and wallets ──────────────────────────────────────────────


def test_r06_wallet_balance_cannot_go_negative(db):
    with rejected("CHECK constraint failed"):
        db.execute("UPDATE wallets SET balance_minor = -1 WHERE wallet_id = 10")


def test_r07_transfer_amount_must_be_positive(db):
    with rejected("CHECK constraint failed"):
        insert(db, "transfers", **transfer(transfer_id=500, amount_minor=0))


def test_r07_ledger_amount_must_be_positive(db):
    with rejected("CHECK constraint failed"):
        insert(db, "ledger_entries", **ledger(amount_minor=0))


def test_r08_transfer_to_same_wallet_is_rejected(db):
    with rejected("CHECK constraint failed"):
        insert(db, "transfers", **transfer(transfer_id=500, from_wallet_id=10, to_wallet_id=10))


# ── R09–R11 lifecycle fields ─────────────────────────────────────────────────


def test_r09_settlement_cannot_precede_creation(db):
    with rejected("CHECK constraint failed"):
        db.execute(
            "UPDATE transfers SET status = 'completed', settled_at = ? WHERE transfer_id = 100",
            (EARLIER,),
        )


def test_r10_failed_transfer_requires_reason(db):
    with rejected("CHECK constraint failed"):
        db.execute("UPDATE transfers SET status = 'failed' WHERE transfer_id = 100")


def test_r10_non_failed_transfer_cannot_have_reason(db):
    with rejected("CHECK constraint failed"):
        db.execute("UPDATE transfers SET failure_reason = 'x' WHERE transfer_id = 100")


def test_r11_completed_transfer_requires_settled_at(db):
    with rejected("CHECK constraint failed"):
        db.execute("UPDATE transfers SET status = 'completed' WHERE transfer_id = 100")


def test_r11_pending_transfer_cannot_have_settled_at(db):
    with rejected("CHECK constraint failed"):
        db.execute("UPDATE transfers SET settled_at = ? WHERE transfer_id = 100", (LATER,))


# ── R12 status transitions ───────────────────────────────────────────────────


def test_r12_failed_transfer_cannot_become_completed(db):
    db.execute(
        "UPDATE transfers SET status = 'failed', failure_reason = 'x' WHERE transfer_id = 100"
    )
    with rejected("R12"):
        db.execute(
            "UPDATE transfers SET status = 'completed', failure_reason = NULL, settled_at = ? "
            "WHERE transfer_id = 100",
            (LATER,),
        )


def test_r12_pending_transfer_cannot_skip_to_reversed(db):
    with rejected("R12"):
        db.execute(
            "UPDATE transfers SET status = 'reversed', settled_at = ? WHERE transfer_id = 100",
            (LATER,),
        )


def test_r12_reversed_is_terminal(db):
    db.execute("UPDATE transfers SET status = 'reversed' WHERE transfer_id = 200")
    with rejected("R12"):
        db.execute(
            "UPDATE transfers SET status = 'pending', settled_at = NULL WHERE transfer_id = 200"
        )


# ── R13 append-only history ──────────────────────────────────────────────────

HISTORY_REWRITES = [
    pytest.param("UPDATE ledger_entries SET amount_minor = 1", id="ledger_update"),
    pytest.param("DELETE FROM ledger_entries", id="ledger_delete"),
    pytest.param("UPDATE transfer_status_events SET to_status = 'failed'", id="events_update"),
    pytest.param("DELETE FROM transfer_status_events", id="events_delete"),
]


@pytest.mark.parametrize("statement", HISTORY_REWRITES)
def test_r13_history_tables_are_append_only(db, statement):
    with rejected("R13"):
        db.execute(statement)


# ── R16 customer deletes ─────────────────────────────────────────────────────


def test_r16_active_customer_cannot_be_deleted(db):
    insert(db, "customers", **customer(customer_id=4, email="d@example.com"))
    with rejected("R16"):
        db.execute("DELETE FROM customers WHERE customer_id = 4")


def test_r16_customer_with_wallet_cannot_be_deleted(db):
    db.execute("UPDATE customers SET status = 'abandoned' WHERE customer_id = 1")
    with rejected("FOREIGN KEY constraint failed"):
        db.execute("DELETE FROM customers WHERE customer_id = 1")


def test_r16_abandoned_customer_without_wallet_can_be_deleted(db):
    db.execute("DELETE FROM customers WHERE customer_id = 3")
    assert db.execute("SELECT COUNT(*) FROM customers WHERE customer_id = 3").fetchone()[0] == 0


# ── R17 customer identity ────────────────────────────────────────────────────


def test_r17_customer_needs_email_or_phone(db):
    with rejected("CHECK constraint failed"):
        insert(db, "customers", **customer(customer_id=50))


def test_r17_email_is_unique(db):
    with rejected("UNIQUE constraint failed"):
        insert(db, "customers", **customer(customer_id=50, email="a@example.com"))


def test_r17_phone_is_unique(db):
    with rejected("UNIQUE constraint failed"):
        insert(db, "customers", **customer(customer_id=50, phone="+971500000003"))
