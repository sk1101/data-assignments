-- Source system of record: wallet / payments domain.

CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    email TEXT UNIQUE,
    phone TEXT UNIQUE,
    full_name TEXT NOT NULL,
    country_code TEXT NOT NULL,
    kyc_status TEXT NOT NULL DEFAULT 'pending',
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CONSTRAINT r04_customer_country CHECK (country_code GLOB '[A-Z][A-Z]'),
    CONSTRAINT r04_customer_kyc_status
    CHECK (kyc_status IN ('pending', 'verified', 'rejected')),
    CONSTRAINT r04_customer_status CHECK (status IN ('active', 'abandoned', 'closed')),
    CONSTRAINT r17_customer_identifier CHECK (email IS NOT NULL OR phone IS NOT NULL)
) STRICT;

CREATE TABLE wallets (
    wallet_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers (customer_id),
    currency TEXT NOT NULL,
    balance_minor INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CONSTRAINT r05_one_wallet_per_currency UNIQUE (customer_id, currency),
    CONSTRAINT r04_wallet_currency CHECK (currency IN ('AED', 'USD', 'INR')),
    CONSTRAINT r04_wallet_status CHECK (status IN ('active', 'frozen', 'closed')),
    CONSTRAINT r06_balance_not_negative CHECK (balance_minor >= 0)
) STRICT;

CREATE TABLE transfers (
    transfer_id INTEGER PRIMARY KEY,
    from_wallet_id INTEGER NOT NULL REFERENCES wallets (wallet_id),
    to_wallet_id INTEGER NOT NULL REFERENCES wallets (wallet_id),
    amount_minor INTEGER NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    settled_at TEXT,
    updated_at TEXT NOT NULL,
    CONSTRAINT r04_transfer_currency CHECK (currency IN ('AED', 'USD', 'INR')),
    CONSTRAINT r04_transfer_status
    CHECK (status IN ('pending', 'completed', 'failed', 'reversed')),
    CONSTRAINT r07_transfer_amount_positive CHECK (amount_minor > 0),
    CONSTRAINT r08_distinct_wallets CHECK (from_wallet_id <> to_wallet_id),
    CONSTRAINT r09_settled_after_created
    CHECK (settled_at IS NULL OR settled_at >= created_at),
    CONSTRAINT r10_failure_reason_iff_failed
    CHECK ((status = 'failed') = (failure_reason IS NOT NULL)),
    CONSTRAINT r11_settled_at_iff_settled
    CHECK ((status IN ('completed', 'reversed')) = (settled_at IS NOT NULL))
) STRICT;

CREATE TABLE ledger_entries (
    transfer_id INTEGER NOT NULL REFERENCES transfers (transfer_id),
    entry_no INTEGER NOT NULL,
    wallet_id INTEGER NOT NULL REFERENCES wallets (wallet_id),
    direction TEXT NOT NULL,
    amount_minor INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (transfer_id, entry_no),
    CONSTRAINT r04_ledger_entry_no CHECK (entry_no > 0),
    CONSTRAINT r04_ledger_direction CHECK (direction IN ('debit', 'credit')),
    CONSTRAINT r07_ledger_amount_positive CHECK (amount_minor > 0)
) STRICT;

CREATE TABLE transfer_status_events (
    transfer_id INTEGER NOT NULL REFERENCES transfers (transfer_id),
    seq INTEGER NOT NULL,
    from_status TEXT,
    to_status TEXT NOT NULL,
    changed_at TEXT NOT NULL,
    PRIMARY KEY (transfer_id, seq),
    CONSTRAINT r04_event_seq CHECK (seq > 0),
    CONSTRAINT r04_event_from_status
    CHECK (from_status IN ('pending', 'completed', 'failed', 'reversed')),
    CONSTRAINT r04_event_to_status
    CHECK (to_status IN ('pending', 'completed', 'failed', 'reversed'))
) STRICT;

-- ── Indexes (rationale in docs/01_source_schema.md) ─────────────────────────
-- wallets(customer_id) is already covered by r05_one_wallet_per_currency.

CREATE INDEX ix_transfers_from_wallet ON transfers (from_wallet_id);
CREATE INDEX ix_transfers_to_wallet ON transfers (to_wallet_id);
CREATE INDEX ix_transfers_status_created ON transfers (status, created_at);
CREATE INDEX ix_ledger_wallet_created ON ledger_entries (wallet_id, created_at);
CREATE INDEX ix_customers_updated ON customers (updated_at);
CREATE INDEX ix_wallets_updated ON wallets (updated_at);
CREATE INDEX ix_transfers_updated ON transfers (updated_at);

-- ── R12 allowed status transitions ───────────────────────────────────────────

CREATE TRIGGER r12_transfer_status_transition
BEFORE UPDATE OF status ON transfers
WHEN
    new.status <> old.status
    AND NOT (
        (old.status = 'pending' AND new.status IN ('completed', 'failed'))
        OR (old.status = 'completed' AND new.status = 'reversed')
    )
BEGIN
    SELECT RAISE(ABORT, 'R12: invalid transfer status transition');
END;

-- ── R13 history tables are append-only ───────────────────────────────────────

CREATE TRIGGER r13_ledger_no_update BEFORE UPDATE ON ledger_entries
BEGIN
    SELECT RAISE(ABORT, 'R13: ledger_entries are append-only');
END;

CREATE TRIGGER r13_ledger_no_delete BEFORE DELETE ON ledger_entries
BEGIN
    SELECT RAISE(ABORT, 'R13: ledger_entries are append-only');
END;

CREATE TRIGGER r13_events_no_update BEFORE UPDATE ON transfer_status_events
BEGIN
    SELECT RAISE(ABORT, 'R13: transfer_status_events are append-only');
END;

CREATE TRIGGER r13_events_no_delete BEFORE DELETE ON transfer_status_events
BEGIN
    SELECT RAISE(ABORT, 'R13: transfer_status_events are append-only');
END;

-- ── R16 only abandoned customers may be hard-deleted ─────────────────────────
-- (Customers with wallets are additionally protected by the R02 foreign key.)

CREATE TRIGGER r16_only_abandoned_customers_deleted
BEFORE DELETE ON customers
WHEN old.status <> 'abandoned'
BEGIN
    SELECT RAISE(ABORT, 'R16: only abandoned customers can be deleted');
END;
