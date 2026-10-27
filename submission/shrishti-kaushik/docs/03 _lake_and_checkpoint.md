# 03 — Lake and Checkpoint

## Purpose

The lake is the permanent, append-only history of every source change. It is
never updated in place. If the warehouse is lost or corrupted, it can be
rebuilt entirely from the lake.

## Layout

    data/
      lake/changes/lsn_<first>_<last>.parquet   # one immutable file per batch
      state/checkpoint.json                     # last lsn safely in the lake

- One file per batch, containing changes from all tables, in lsn order.
- File names encode the lsn range (zero-padded to 12 digits so that name
  order equals lsn order).
- Files are written once and never modified or deleted.

## Lake record schema

| Column | Type | Meaning |
|---|---|---|
| lsn | int64 | From `_cdc_log`; unique, increasing |
| table_name | string | Source table |
| op | string | `I`, `U`, `D` |
| pk | string (JSON) | Primary key of the row |
| before | string (JSON), nullable | Row before the change |
| after | string (JSON), nullable | Row after the change |
| captured_at | string | UTC time the change was captured in the source |
| batch_id | string | File the record belongs to, e.g. `lsn_..._...` |
| ingested_at | string | UTC time the batch was written to the lake |

`before` and `after` are stored as JSON text rather than typed columns. The lake
is the raw layer: it must keep every change even if the source schema evolves,
and typing happens in the warehouse. DuckDB can still query inside the JSON.

## Ingestion cycle

1. **Resume position** = max(checkpoint, highest lsn in any lake file name).
2. Read up to `batch_size` changes with lsn > resume position.
3. Write them to a temporary file, then rename it to its final name (atomic).
4. Save the checkpoint (also temp file + rename).
5. Optionally prune `_cdc_log` rows with lsn ≤ checkpoint.

## Reliability

| Scenario | Handling |
|---|---|
| Crash before writing a batch | Nothing written; the restart re-reads the same changes |
| Crash while writing a batch | Only a `.tmp` file exists; it is ignored and overwritten |
| Crash after writing, before checkpoint | Resume position comes from the lake file names, so the batch is not written twice |
| Stale or missing checkpoint | Same: the lake file names are the source of truth for progress |
| Duplicate change records | Changes with lsn ≤ resume position are never written; downstream readers also de-duplicate by lsn |
| Restart after checkpointed progress | Continues from the checkpoint; no re-reading |
| Source log pruning | Only rows ≤ the checkpoint are pruned, i.e. only rows already durable in the lake |

Delivery is **at-least-once** (the checkpoint is saved after the data, so a crash
can only cause a repeat, never a loss), and writes are **idempotent** (a repeat
is detected and skipped). Together these give effectively exactly-once results.

## Simulated vs production

| Here | Production equivalent |
|---|---|
| Local Parquet files | Object storage (S3/GCS) with a table format such as Iceberg or Delta Lake, whose commits are atomic |
| Temp file + rename | The table format's atomic commit |
| `checkpoint.json` | Kafka consumer offsets / connector offset storage |
| One file per batch | Same, plus periodic compaction of small files |

## Known limitations

- Many small batches create many small files. Production would compact them.
- The lake keeps personal data of deleted customers forever, which conflicts with
  right-to-erasure rules. Production would mask or encrypt personal fields so
  they can be destroyed (crypto-shredding).
- A single pipeline process is assumed. Two concurrent writers would need a lock.
