# 06 — Validation Parity

## Purpose

Every rule the source enforces (docs/01, R01-R17) is asserted again in the
warehouse, so downstream consumers are protected even if a source constraint is
missing, a rule was only enforced by the application, or the pipeline itself
has a bug.

## How checks work

Each check is a SQL query against the warehouse that returns the rows **breaking**
a rule. Zero rows means the check passes. Every check is tagged with its rule ID.

- **System rules (R01-R04)** are generated from the contract (keys, foreign keys,
  required columns, allowed values), so they follow contract changes automatically.
- **Business rules (R05-R17)** are written by hand. Some use the `history` schema,
  which lets the warehouse verify rules across a row's whole lifetime.
- **C01 (completeness)** checks that the warehouse has applied every change in the lake.

## Parity table

| Rule | Source enforcement | Warehouse check |
|---|---|---|
| R01 Unique primary keys | PRIMARY KEY | No duplicate keys in each `curated` table |
| R02 Foreign keys | FOREIGN KEY | No child row without its parent, per relationship |
| R03 Required columns | NOT NULL | No NULL in any required column |
| R04 Allowed values and formats | CHECK, STRICT | Values in each contract enum; country code is two capital letters; types enforced by typed warehouse columns |
| R05 One wallet per currency | UNIQUE | No customer with two wallets in one currency |
| R06 Non-negative balance | CHECK | No negative balance |
| R07 Positive amounts | CHECK | No zero or negative transfer or ledger amount |
| R08 Distinct wallets | CHECK | No transfer from a wallet to itself |
| R09 Settlement after creation | CHECK | No `settled_at` before `created_at` |
| R10 Failure reason iff failed | CHECK | Reason present exactly when failed |
| R11 Settlement time iff settled | CHECK | `settled_at` present exactly when completed or reversed |
| R12 Allowed transitions | TRIGGER | Every consecutive pair of transfer versions in `history`, and every status event, is an allowed transition |
| R13 Append-only history | TRIGGER | Ledger entries and status events have only insert changes in `history` |
| R14 Currency matches wallets | Application only | Transfer currency equals both wallets' currency |
| R15 Balanced ledger | Application only | Debits = credits = amount (completed), 2 × amount (reversed), none (pending/failed) |
| R16 Only abandoned customers deleted | TRIGGER + FK | Every customer tombstone in `history` was `abandoned` |
| R17 Customer identifier | CHECK + UNIQUE | Email or phone present; no duplicate email; no duplicate phone |
| C01 Completeness | — | Warehouse watermark equals the highest lsn in the lake |

A test asserts that every rule ID in this table has at least one check, so the
parity cannot silently drift.

## How failures are surfaced

- Each result records the rule, the check name, the number of failing rows and a
  sample of up to 5 failing rows, so an investigator sees exactly which records broke it.
- `scripts/run_data_quality_checks.py` (CI) prints a ✓/✗ line per check and exits 1
  on any failure.
- The pipeline runner writes `data/state/quality_report.json` after each cycle and
  logs an ERROR per failing rule.
- Policy: failing data is **reported, not hidden**. The warehouse still reflects
  the source faithfully. Failures in R14/R15 point to an application bug at the
  source; failures in source-enforced rules point to a missing constraint or a
  pipeline bug.

## Simulated vs production

| Here | Production equivalent |
|---|---|
| SQL checks in `quality/checks.py` | dbt tests, Great Expectations or Soda checks |
| JSON report + error log + exit code | Test results gating downstream models, with alerts to the owning team |
