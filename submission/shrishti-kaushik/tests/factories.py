"""Shared test data: valid row builders and a small seeded world.

Each builder returns a VALID row; tests override only the thing they test.
"""

TS = "2026-10-27T10:00:00Z"
LATER = "2026-10-27T10:05:00Z"
EARLIER = "2026-10-27T09:00:00Z"


def insert(conn, table, **values):
    cols = ", ".join(values)
    marks = ", ".join("?" for _ in values)
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({marks})", tuple(values.values()))


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


def seed(conn):
    """A small, valid world, with at least one row in every table:
    customers 1, 2 (email) and 3 (phone only, abandoned, no wallet)
    wallets 10 (customer 1, AED 50.00) and 20 (customer 2, empty)
    transfer 100: pending, one status event, no ledger lines
    transfer 200: completed, two status events, two ledger lines
    """
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
    conn.commit()
