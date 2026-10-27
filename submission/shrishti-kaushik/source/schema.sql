-- Source system of record: wallet / payments domain.

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    email TEXT,
    phone TEXT,
    full_name TEXT,
    country_code TEXT,
    kyc_status TEXT,
    status TEXT,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE wallets (
    wallet_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    currency TEXT,
    balance_minor INTEGER,
    status TEXT,
    created_at TEXT,
    updated_at TEXT
);

CREATE TABLE transfers (
    transfer_id INTEGER PRIMARY KEY,
    from_wallet_id INTEGER,
    to_wallet_id INTEGER,
    amount_minor INTEGER,
    currency TEXT,
    status TEXT,
    failure_reason TEXT,
    created_at TEXT,
    settled_at TEXT,
    updated_at TEXT
);

CREATE TABLE ledger_entries (
    transfer_id INTEGER,
    entry_no INTEGER,
    wallet_id INTEGER,
    direction TEXT,
    amount_minor INTEGER,
    created_at TEXT,
    PRIMARY KEY (transfer_id, entry_no)
);

CREATE TABLE transfer_status_events (
    transfer_id INTEGER,
    seq INTEGER,
    from_status TEXT,
    to_status TEXT,
    changed_at TEXT,
    PRIMARY KEY (transfer_id, seq)
);
