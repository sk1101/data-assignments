# CDC Lakehouse: Payments Wallet

A change data capture pipeline that keeps a **lake** (every change, forever) and
a **warehouse** (current state, plus full history for time travel and restore)
in sync with a transactional payments database, and **stops safely** when the
source schema changes.

```
 SOURCE (SQLite)                LAKE (Parquet)              WAREHOUSE (DuckDB)
 customers, wallets,  triggers  append-only change  apply   history.*  (SCD2: every version)
 transfers, ledger, ─────────▶  files, one per   ───────▶  curated.* (current-state views)
 status events        _cdc_log  lsn-range batch    by lsn   meta.applied (watermark)
        │                                                         │
        └── schema guard: live schema vs contract, every cycle    └── quality checks (R01–R17, C01)
                                                                      catalog (lake + warehouse)
```

## Quickstart

Requires Python 3.11+ (with SQLite 3.37+, for `STRICT` tables).

```bash
cd submission/shrishti-kaushik
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pytest -q                                   # 160 tests
python run.py demo                          # the whole story end to end
```

The CI checks, run exactly as the workflow does (from the repo root):

```bash
python submission/shrishti-kaushik/scripts/check_schema_contracts.py
python submission/shrishti-kaushik/scripts/run_data_quality_checks.py
python submission/shrishti-kaushik/scripts/validate_catalog.py
```

## Running the pipeline

```bash
python run.py init                 # data/source.db with CDC installed and sample activity
python run.py once                 # one cycle: guard → lake → warehouse → quality report
python run.py watch --interval 2   # near real time: a cycle every 2 seconds
```

Exit codes: `0` OK, `1` quality checks failed, `2` stopped by the schema guard.
State is written to `data/state/`: `checkpoint.json`, `quality_report.json`,
and `schema_alert.json` when the guard stops ingestion.

## What the demo shows

```
── 1. Sample activity captured by CDC; first cycle builds lake and warehouse
   ingested 36 changes, applied 36, failed checks: 0
── 2. New activity: Aisha sends Rohan AED 100.00; one cycle later it is live
   8 changes applied in 0.04s
── 3. Re-running with nothing new changes nothing
   ingested 0, applied 0; lake holds 44 changes, 44 distinct
── 4. Time travel: wallet balances as of lsn 36 (before step 2)
── 5. Restore: Lena's customer record was deleted; recover it from history
   restore_lsn_35.customers → (3, 'Lena Fischer', 'lena@example.com', 'abandoned')
── 6. The source team adds a column without warning: the pipeline stops
   STOPPED: source schema does not match the contract:
     - unexpected_column: customers.loyalty_tier (found {'type': 'TEXT', 'nullable': True})
   lake files before: 2, after: 2
```

## Querying the data

```bash
python -c "import duckdb; duckdb.connect('data/demo/warehouse.duckdb').sql('SELECT * FROM curated.wallets').show()"
python -c "import duckdb; duckdb.sql(\"SELECT lsn, table_name, op, pk FROM read_parquet('data/demo/lake/changes/*.parquet') ORDER BY lsn\").show()"
```

Every dataset, with its owner, intended consumers, cadence, retention, columns
and an example query, is listed in [`catalog/catalog.json`](catalog/catalog.json).

## Requirements → implementation → proof

| Requirement | Where | Proven by |
|---|---|---|
| Source schema: strong/weak entities, keys, indexes, types, rules | `source/schema.sql`, docs/01 | `tests/test_source_constraints.py` (every key, column and enum) |
| Capture every insert, update, delete | `pipeline/capture.py` (generated triggers), docs/02 | `tests/test_cdc_capture.py` |
| Lake keeps every change; replay and restart safe | `pipeline/lake.py`, `pipeline/ingest.py`, docs/03 | `tests/test_lake_ingestion.py` (crash mid-write, crash before checkpoint, lost/rewound checkpoint, pruning) |
| Stop on incompatible schema change | `pipeline/schema_guard.py`, `contracts/source_contract.json`, docs/04 | `tests/test_schema_guard.py`, `scripts/check_schema_contracts.py` |
| Warehouse latest snapshot, near real time | `pipeline/warehouse.py` (`curated` views), docs/05 | `tests/test_warehouse.py`, `tests/test_end_to_end.py` |
| Time travel and restore | `history` (SCD2), `as_of`, `restore_point`, `rebuild` | `tests/test_warehouse.py` (every past state reconstructed exactly) |
| Validation parity | `quality/checks.py`, docs/06 | `tests/test_data_quality.py` (every rule in docs/01 has a check; each check catches its rule) |
| Catalog | `catalog/catalog.json`, `pipeline/catalog.py`, docs/07 | `tests/test_catalog.py`, `scripts/validate_catalog.py` |

## Repository layout

```
docs/          design docs 01–07, written before the code for each layer
source/        the source system: schema, connection helper, sample activity
contracts/     the schema contract the pipeline was built for (reviewed, versioned)
pipeline/      capture → ingest (lake + checkpoint) → schema guard → warehouse → catalog
quality/       warehouse checks mirroring every source rule
catalog/       published metadata for every lake and warehouse dataset
scripts/       CI entry points, plus contract and catalog generators
tests/         164 tests across every layer
run.py         runner: demo, init, once, watch
```

## Design docs

1. [Source schema](docs/01_source_schema.md): entities, rules R01–R17, change patterns
2. [CDC contract](docs/02_cdc_contract.md): what a change is; guarantees; simulated vs production
3. [Lake and checkpoint](docs/03_lake_and_checkpoint.md): layout, crash table, exactly-once
4. [Schema contract](docs/04_schema_contract.md): compatibility policy, stop-the-line, runbook
5. [Warehouse](docs/05_warehouse.md): SCD2 history, current views, time travel, restore
6. [Validation parity](docs/06_validation_parity.md): rule → check mapping, reporting
7. [Catalog](docs/07_catalog.md): datasets, metadata, discovery

## Key assumptions and limitations

- **Simulated CDC:** SQLite triggers write to `_cdc_log`. Production would use log-based
  CDC (e.g. Debezium on the Postgres WAL) via Kafka. Triggers add write overhead and
  are coupled to the schema, which is why every schema change stops ingestion.
- **Single writer:** SQLite's single writer guarantees lsn order equals commit order.
- **Polling:** `watch` polls every N seconds; production would stream continuously.
- **Lake:** local Parquet with atomic renames stands in for S3 + Iceberg/Delta. Small
  files are not compacted, and personal data in the lake is not erasable (crypto-shredding
  would be needed).
- **Warehouse:** changes are applied row by row (production: a set-based `MERGE`);
  timestamps stay as ISO-8601 text; columns appear in alphabetical order; a contract
  version change needs a matching warehouse migration.
- **Schema guard:** allowed values are detected only for `CHECK (col IN (...))` rules;
  other rule changes are caught by the DDL fingerprint, which also trips on
  formatting-only edits (a deliberate false-positive bias).
