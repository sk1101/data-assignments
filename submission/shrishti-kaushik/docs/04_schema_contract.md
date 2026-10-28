# 04 — Schema Contract and Stop-the-Line

## Purpose

The source team gives no backward-compatibility guarantee. The pipeline must
detect any change to the source schema and stop before ingesting data under
broken assumptions. We **fail closed**: an unrecognised change stops ingestion
and alerts a human, rather than letting data through.

## The contract

`contracts/source_contract.json` records the schema the pipeline was built for:

| Per table | Example |
|---|---|
| Columns, with type and nullability | `balance_minor: INTEGER, not null` |
| Primary key | `["transfer_id", "entry_no"]` |
| Foreign keys | `wallets.customer_id → customers.customer_id` |
| Allowed values (enum domains) | `kyc_status: pending, verified, rejected` |
| DDL fingerprint | SHA-256 of the normalised `CREATE TABLE` text |

The contract is generated once from the current schema, **reviewed by a person,
and committed**. From then on it changes only through a reviewed pull request,
with its `version` bumped. That review step is what makes it a contract rather
than a snapshot.

The same file is the single source of truth for our own code: the source rule
tests, the schema guard and the warehouse checks all read allowed values and
required columns from it.

## Compatibility policy

Every difference between the live schema and the contract stops ingestion:

| Violation | Example |
|---|---|
| `missing_table` / `unexpected_table` | A table dropped / a new table created |
| `missing_column` / `unexpected_column` | A column dropped / added. A rename shows as both. |
| `type_changed` | `balance_minor` INTEGER → TEXT |
| `nullability_changed` | `full_name` NOT NULL → nullable (or the reverse) |
| `enum_changed` | `kyc_status` gains `expired` |
| `primary_key_changed` | Key columns added, removed or reordered |
| `foreign_key_changed` | A relationship added, removed or retargeted |
| `ddl_changed` | Any other change to the table definition, e.g. a CHECK rule loosened |

Additive changes (a new column or table) also stop ingestion. Capture triggers
are generated from the column list at install time, so a new column or table
would otherwise be silently missing from the change stream.

## Detection

- **When:** at the start of every ingestion cycle, before any change is read;
  and in CI (`scripts/check_schema_contracts.py`).
- **How:** the live schema is read from SQLite's catalogue (`PRAGMA table_info`,
  `PRAGMA foreign_key_list`, `sqlite_master`). Allowed values are parsed from
  `CHECK (column IN (...))` rules. A fingerprint of each normalised
  `CREATE TABLE` statement catches any change not covered by the specific checks.

## Stop-the-line behaviour

On any violation:

1. The cycle raises `SchemaContractViolation` **before reading any change**:
   nothing is written to the lake or warehouse, and the checkpoint does not move.
2. An alert file is written to `data/state/schema_alert.json`, listing every
   violation (table, column, kind, expected vs actual) and the time detected.
3. An `ERROR` is logged, and the pipeline runner exits with a non-zero code,
   so a scheduler or supervisor marks the run as failed.

The check repeats on every cycle, so ingestion stays stopped until the schema
and the contract agree again.

## Recovery runbook

1. Read `schema_alert.json` to see exactly what changed.
2. Agree the change with the source team.
3. Update the contract in a pull request and bump its `version`.
4. Reinstall capture triggers so they match the new column list.
5. Update the warehouse model and checks if needed.
6. Resume. Changes captured before the schema change are still in `_cdc_log`
   (it is only pruned after successful ingestion) and flow through normally.

## Simulated vs production

| Here | Production equivalent |
|---|---|
| JSON contract file in the repo | A schema registry (e.g. Confluent Schema Registry) with an enforced compatibility mode |
| Guard reading SQLite's catalogue | Debezium schema-change events, checked against the registry |
| Alert file + error log + exit code | Alerts routed to on-call (Slack, PagerDuty); job marked failed |

## Known limitations

- Allowed values are detected only for rules written as `CHECK (column IN (...))`.
  Other rule shapes are covered by the DDL fingerprint, which detects that
  something changed but not exactly what.
- The fingerprint also trips on formatting-only edits to a table definition.
  This is deliberate: a false alarm is cheaper than silent drift.
