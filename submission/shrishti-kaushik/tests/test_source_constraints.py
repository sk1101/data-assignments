"""Source constraint tests: the source must reject every invalid state in docs/01.

Each test names the rule it checks (R01-R17) and asserts the *reason* for the
rejection, so a test cannot pass because some other rule happened to fire.
R14 and R15 span several rows/tables and are enforced by the application,
so they are asserted in the warehouse rather than here.
"""

import sqlite3
from pathlib import Path

import pytest

SCHEMA = Path(__file__).resolve().parent.parent / "source" / "schema.sql"
TS = "2026-10-27T10:00:00Z"
LATER = "2026-10-27T10:05:00Z"
EARLIER = "2026-10-27T09:00:00Z"


def insert(conn, table, **values):
    cols = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", tuple(values.values()))


def rejected(reason):
    """The write inside this block must be refused, with `reason` in the error."""
    return pytest.raises(sqlite3.IntegrityError, match=reason)


def customer(**overrides):
    row = {
        "email": None,
        "phone": None,
        "full_name": "Test User",
        "country_code": "AE",
        "kyc_status": "verified",
        "status": "active",
        "created_at": TS,
        "updated_at": TS,
    }
    return {**row, **overrides}


def wallet(**overrides):
    row = {
        "currency": "AED",
        "balance_minor": 0,
        "status": "active",
        "created_at": TS,
        "updated_at": TS,
    }
    return {**row, **overrides}


def transfer(**overrides):
    row = {
        "from_wallet_id": 10,
        "to_wallet_id": 20,
        "amount_minor": 1000,
        "currency": "AED",
        "status": "pending",
        "failure_reason": None,
        "created_at": TS,
        "settled_at": None,
        "updated_at": TS,
    }
    return {**row, **overrides}


def ledger(**overrides):
    row = {
        "transfer_id": 200,
        "entry_no": 3,
        "wallet_id": 10,
        "direction": "debit",
        "amount_minor": 500,
        "created_at": TS,
    }
    return {**row, **overrides}


def event(**overrides):
    row = {
        "transfer_id": 100,
        "seq": 1,
        "from_status": None,
        "to_status": "pending",
        "changed_at": TS,
    }
    return {**row, **overrides}


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
    """A small, valid world:
    customers 1, 2 (email) and 3 (phone only, abandoned, no wallet)
    wallets 10 (customer 1, AED 50.00) and 20 (customer 2, empty)
    transfer 100: pending, one status event, no ledger lines
    transfer 200: completed, two status events, two ledger lines
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA.read_text())
    insert(conn, "customers", **customer(customer_id=1, email="a@example.com"))
    insert(conn, "customers", **customer(customer_id=2, email="b@example.com"))
    insert(conn, "customers", **customer(customer_id=3, phone="+971500000003", status="abandoned"))
    insert(conn, "wallets", **wallet(wallet_id=10, customer_id=1, balance_minor=5000))
    insert(conn, "wallets", **wallet(wallet_id=20, customer_id=2))
    insert(conn, "transfers", **transfer(transfer_id=100))
    insert(conn, "transfer_status_events", **event(transfer_id=100, seq=1))
    insert(
        conn,
        "transfers",
        **transfer(transfer_id=200, amount_minor=500, status="completed", settled_at=LATER),
    )
    insert(conn, "transfer_status_events", **event(transfer_id=200, seq=1))
    insert(
        conn,
        "transfer_status_events",
        **event(transfer_id=200, seq=2, from_status="pending", to_status="completed"),
    )
    insert(conn, "ledger_entries", **ledger(entry_no=1, wallet_id=10, direction="debit"))
    insert(conn, "ledger_entries", **ledger(entry_no=2, wallet_id=20, direction="credit"))
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

REQUIRED_COLUMNS = {
    "customers": ["full_name", "country_code", "kyc_status", "status", "created_at", "updated_at"],
    "wallets": ["customer_id", "currency", "balance_minor", "status", "created_at", "updated_at"],
    "transfers": [
        "from_wallet_id",
        "to_wallet_id",
        "amount_minor",
        "currency",
        "status",
        "created_at",
        "updated_at",
    ],
    "ledger_entries": [
        "transfer_id",
        "entry_no",
        "wallet_id",
        "direction",
        "amount_minor",
        "created_at",
    ],
    "transfer_status_events": ["transfer_id", "seq", "to_status", "changed_at"],
}

NOT_NULL_CASES = [
    pytest.param(table, column, id=f"{table}.{column}")
    for table, columns in REQUIRED_COLUMNS.items()
    for column in columns
]


@pytest.mark.parametrize("table, column", NOT_NULL_CASES)
def test_r03_required_column_cannot_be_null(db, table, column):
    row = NEW_ROW[table]()
    row[column] = None
    with rejected("NOT NULL constraint failed"):
        insert(db, table, **row)


# ── R04 allowed values and types ─────────────────────────────────────────────

BAD_VALUES = [
    pytest.param("customers", "kyc_status", "unknown", id="customers.kyc_status"),
    pytest.param("customers", "status", "deleted", id="customers.status"),
    pytest.param("customers", "country_code", "UAE", id="customers.country_code"),
    pytest.param("wallets", "currency", "EUR", id="wallets.currency"),
    pytest.param("wallets", "status", "suspended", id="wallets.status"),
    pytest.param("transfers", "currency", "EUR", id="transfers.currency"),
    pytest.param("transfers", "status", "refunded", id="transfers.status"),
    pytest.param("ledger_entries", "direction", "sideways", id="ledger_entries.direction"),
    pytest.param("transfer_status_events", "from_status", "refunded", id="events.from_status"),
    pytest.param("transfer_status_events", "to_status", "refunded", id="events.to_status"),
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
