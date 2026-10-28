# 05 — Warehouse: Current Snapshot, History and Restore

## Purpose

The warehouse gives analysts the current state of every source table within
seconds of a change, and lets anyone reconstruct or restore the state at any
earlier point. It is built **only from the lake**, so it can always be rebuilt
from scratch by replaying the lake.

Engine: DuckDB, file `data/warehouse.duckdb`.

## Model

| Schema | Objects | Purpose |
|---|---|---|
| `history` | One table per source table | Every version of every row (SCD Type 2) |
| `curated` | One view per source table | Current state: the latest non-deleted version of each row |
| `meta` | `applied` | Warehouse watermark: highest lsn applied |
| `restore_lsn_<N>` | Created on demand | Full copy of every table as of lsn N |

### History tables

Each history table has the source table's columns (typed from the contract:
INTEGER → BIGINT, TEXT → VARCHAR) plus:

| Column | Meaning |
|---|---|
| `_lsn` | lsn of the change that created this version |
| `_op` | `I`, `U` or `D` |
| `_is_deleted` | True for the tombstone version written by a delete |
| `_valid_from` | `captured_at` of the change that created this version |
| `_valid_to`, `_valid_to_lsn` | `captured_at` and lsn of the next change to the same row; NULL while current |

A delete closes the current version and adds a tombstone carrying the deleted
row's values, so history is never lost.

### Current views

`curated.<table>` returns the source columns of versions where
`_valid_to_lsn IS NULL AND NOT _is_deleted`. Because it is a view over history,
the current state can never drift from the history it is derived from.

## Applying changes

1. Read lake records with lsn > the watermark (whole lake files at or below the
   watermark are skipped using their file names).
2. De-duplicate by lsn and sort by lsn.
3. For each change: close the current version of that row, then insert the new
   version (or tombstone).
4. Move the watermark.

Steps 3 and 4 run in **one transaction**: a crash rolls back the whole batch, and
the next run re-applies it cleanly. Records at or below the watermark are
duplicates by construction (lsns are unique and increasing) and are skipped.

## Time travel and restore

- **As of an lsn:** a version is visible at lsn N if `_lsn <= N` and
  (`_valid_to_lsn IS NULL` or `_valid_to_lsn > N`), and it is not a tombstone.
- **As of a time:** converted to the last lsn whose `captured_at` is at or before
  that time, then queried as above. lsn is the precise reference, since several
  changes can share a timestamp.
- **Restore point:** `restore_point(lsn)` materialises every table as of that lsn
  into a new schema, `restore_lsn_<N>`. Operators compare it with `curated` and
  use it to repair the source (e.g. re-insert rows deleted by mistake).
- **Rebuild:** if the warehouse is lost or corrupted, drop it and replay the lake,
  optionally only up to a given lsn.

## Reliability

| Scenario | Handling |
|---|---|
| Duplicate change records | Skipped: lsn at or below the watermark, or repeated within a batch |
| Out-of-order arrival | Changes are always applied in lsn (commit) order |
| Crash mid-apply | One transaction per batch; the watermark moves only on commit |
| Deletes | Current version closed; tombstone keeps the deleted values |
| Warehouse lost or corrupted | Rebuilt from the lake |

## Simulated vs production

| Here | Production equivalent |
|---|---|
| DuckDB file | Snowflake / BigQuery / Databricks |
| Python applying changes row by row | A set-based `MERGE` per batch (e.g. dbt snapshots or incremental models) |
| `restore_lsn_<N>` schemas | Warehouse-native time travel (e.g. Snowflake `AT`, Delta `VERSION AS OF`) plus SCD2 history for longer retention |

## Known limitations

- Timestamps stay as ISO-8601 UTC text (`VARCHAR`) rather than a timestamp type;
  their fixed format keeps them correctly sortable.
- Warehouse tables are generated from contract v1. A contract version change
  needs a matching warehouse migration.
- Changes are applied one row at a time, which is fine at this scale but slow
  for large backlogs.