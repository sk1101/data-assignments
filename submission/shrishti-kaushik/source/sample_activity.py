"""A small burst of realistic application activity, used by the demo and CI checks.

Each operation runs in one transaction and keeps the application-level rules:
R14 (currency matches both wallets) and R15 (balanced double-entry ledger).
"""

import sqlite3


def at(minute: int) -> str:
    return f"2026-10-28T09:{minute:02d}:00Z"


def _event(conn, transfer_id, seq, from_status, to_status, when):
    conn.execute(
        "INSERT INTO transfer_status_events "
        "(transfer_id, seq, from_status, to_status, changed_at) VALUES (?, ?, ?, ?, ?)",
        (transfer_id, seq, from_status, to_status, when),
    )


def _ledger(conn, transfer_id, entry_no, wallet_id, direction, amount, when):
    conn.execute(
        "INSERT INTO ledger_entries "
        "(transfer_id, entry_no, wallet_id, direction, amount_minor, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (transfer_id, entry_no, wallet_id, direction, amount, when),
    )


def _move(conn, wallet_id, delta, when):
    conn.execute(
        "UPDATE wallets SET balance_minor = balance_minor + ?, updated_at = ? "
        "WHERE wallet_id = ?",
        (delta, when, wallet_id),
    )


def _parties(conn, transfer_id):
    return conn.execute(
        "SELECT from_wallet_id, to_wallet_id, amount_minor FROM transfers WHERE transfer_id = ?",
        (transfer_id,),
    ).fetchone()


def open_customer(conn, customer_id, full_name, when, email=None, phone=None):
    with conn:
        conn.execute(
            "INSERT INTO customers (customer_id, email, phone, full_name, country_code, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, 'AE', ?, ?)",
            (customer_id, email, phone, full_name, when, when),
        )


def open_wallet(conn, wallet_id, customer_id, currency, balance, when):
    with conn:
        conn.execute(
            "INSERT INTO wallets (wallet_id, customer_id, currency, balance_minor, "
            "created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
            (wallet_id, customer_id, currency, balance, when, when),
        )


def start_transfer(conn, transfer_id, from_wallet, to_wallet, amount, when):
    with conn:
        (currency,) = conn.execute(
            "SELECT currency FROM wallets WHERE wallet_id = ?", (from_wallet,)
        ).fetchone()
        conn.execute(
            "INSERT INTO transfers (transfer_id, from_wallet_id, to_wallet_id, amount_minor, "
            "currency, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (transfer_id, from_wallet, to_wallet, amount, currency, when, when),
        )
        _event(conn, transfer_id, 1, None, "pending", when)


def complete_transfer(conn, transfer_id, when):
    with conn:
        from_wallet, to_wallet, amount = _parties(conn, transfer_id)
        _move(conn, from_wallet, -amount, when)
        _move(conn, to_wallet, amount, when)
        _ledger(conn, transfer_id, 1, from_wallet, "debit", amount, when)
        _ledger(conn, transfer_id, 2, to_wallet, "credit", amount, when)
        conn.execute(
            "UPDATE transfers SET status = 'completed', settled_at = ?, updated_at = ? "
            "WHERE transfer_id = ?",
            (when, when, transfer_id),
        )
        _event(conn, transfer_id, 2, "pending", "completed", when)


def fail_transfer(conn, transfer_id, reason, when):
    with conn:
        conn.execute(
            "UPDATE transfers SET status = 'failed', failure_reason = ?, updated_at = ? "
            "WHERE transfer_id = ?",
            (reason, when, transfer_id),
        )
        _event(conn, transfer_id, 2, "pending", "failed", when)


def reverse_transfer(conn, transfer_id, when):
    with conn:
        from_wallet, to_wallet, amount = _parties(conn, transfer_id)
        _move(conn, to_wallet, -amount, when)
        _move(conn, from_wallet, amount, when)
        _ledger(conn, transfer_id, 3, to_wallet, "debit", amount, when)
        _ledger(conn, transfer_id, 4, from_wallet, "credit", amount, when)
        conn.execute(
            "UPDATE transfers SET status = 'reversed', updated_at = ? WHERE transfer_id = ?",
            (when, transfer_id),
        )
        _event(conn, transfer_id, 3, "completed", "reversed", when)


def run_sample_activity(conn: sqlite3.Connection) -> None:
    """Customers, wallets, and transfers that complete, fail and get reversed."""
    open_customer(conn, 1, "Aisha Rahman", at(0), email="aisha@example.com")
    open_customer(conn, 2, "Rohan Mehta", at(1), phone="+971500000002")
    open_customer(conn, 3, "Lena Fischer", at(2), email="lena@example.com")
    open_wallet(conn, 10, 1, "AED", 100_000, at(3))
    open_wallet(conn, 20, 2, "AED", 0, at(4))
    open_wallet(conn, 30, 1, "USD", 5_000, at(5))
    with conn:
        conn.execute(
            "UPDATE customers SET kyc_status = 'verified', updated_at = ? "
            "WHERE customer_id IN (1, 2)",
            (at(6),),
        )

    start_transfer(conn, 100, 10, 20, 25_000, at(10))  # AED 250.00 Aisha → Rohan
    complete_transfer(conn, 100, at(11))
    start_transfer(conn, 101, 20, 10, 500_000, at(12))  # more than Rohan has
    fail_transfer(conn, 101, "insufficient funds", at(13))
    start_transfer(conn, 102, 20, 10, 5_000, at(14))  # AED 50.00 Rohan → Aisha
    complete_transfer(conn, 102, at(15))
    reverse_transfer(conn, 102, at(20))

    with conn:  # Lena never opened a wallet: soft delete, then the retention job
        conn.execute(
            "UPDATE customers SET status = 'abandoned', updated_at = ? WHERE customer_id = 3",
            (at(30),),
        )
    with conn:
        conn.execute("DELETE FROM customers WHERE customer_id = 3")
