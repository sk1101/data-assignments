# 02 — CDC Contract

## What a "change" is

Every committed INSERT, UPDATE or DELETE on a source table produces exactly one
change record per affected row in `_cdc_log`, written by an AFTER trigger in the
same transaction as the change itself.

## Change record

| Field | Type | Meaning |
|---|---|---|
| lsn | INTEGER | Log sequence number. Strictly increasing and never reused. Defines the order of changes. |
| table_name | TEXT | Source table that changed |
| op | TEXT | `I` insert, `U` update, `D` delete |
| pk | TEXT (JSON) | Primary key of the row, e.g. `{"transfer_id": 200, "entry_no": 1}`. Composite keys are supported. |
| before | TEXT (JSON) | Full row before the change. NULL for inserts. |
| after | TEXT (JSON) | Full row after the change. NULL for deletes. |
| captured_at | TEXT | UTC time the change was recorded, `YYYY-MM-DDTHH:MM:SS.sssZ` |

Full before/after images (not just changed columns) are captured so that every
record can be understood on its own, and so deletes keep the deleted row's
values for audit.

## Guarantees

1. **Complete:** every committed change is captured. Capture does not depend on
   application code remembering to log it.
2. **Atomic:** a change and its record commit or roll back together. A rejected
   or rolled-back write leaves no record.
3. **Ordered:** `lsn` order equals commit order (see assumptions).
4. **Stable identity:** an `lsn` is never reused, even after old log rows are
   pruned (`AUTOINCREMENT`). Downstream uses `lsn` to detect duplicates and resume.

## Assumptions

- **Single writer.** SQLite allows one write transaction at a time, so a
  transaction's records get consecutive `lsn`s and commit before the next
  transaction starts. A reader that resumes from "last lsn seen" therefore never
  skips an in-flight change. (In Postgres this does not hold for sequences; a
  production setup reads the WAL, which is commit-ordered.)
- **Primary keys never change.** Rows are identified by `pk`, and updates to
  key columns are not expected.
- **No-op updates** (setting a column to its current value) are still captured.
  They are harmless downstream because applying them is idempotent.
- **Schema changes (DDL) are not captured** as change records. They are detected
  separately by the schema guard (docs/04).

## Simulated vs production

| Here | Production equivalent |
|---|---|
| AFTER triggers writing to `_cdc_log` | Debezium (or similar) reading the Postgres WAL via a logical replication slot |
| `_cdc_log.lsn` | WAL log sequence number (LSN) |
| Pipeline polling `_cdc_log` | Change events streamed through Kafka |
| Triggers installed by `pipeline/capture.py` | Connector configuration owned by the data platform team |

Tradeoff: triggers add write overhead to every source transaction and live
inside the source database. Log-based CDC avoids both, which is why it is the
production choice.
- **Trigger column lists are fixed at install time.** A later source schema
  change is not silently absorbed: the schema guard detects it and
  stops ingestion before any record is read.