# 07 — Catalog

## Purpose

`catalog/catalog.json` lists every lake and warehouse dataset so that consumers
can discover what exists, decide which dataset to use, and query it.

## Datasets

| Dataset | Layer | Intended consumers |
|---|---|---|
| `lake.cdc_changes` | `lake` | Data engineers (replay, rebuild, debugging), auditors |
| `warehouse.curated.<table>` | `warehouse-curated` | Analysts, finance, BI dashboards: the default choice |
| `warehouse.history.<table>` | `warehouse-history` | Auditors; restore and time travel |

## Published metadata

Every entry has: `name`, `layer`, `description`, `location`, `format`, `owner`,
`intended_consumers`, `update_cadence`, `retention`, `primary_key`,
`how_to_query`, and `columns` (name and type). The catalog also records the
source contract version it was built against.

## Discovery and access

1. Open `catalog/catalog.json` (or search it), and pick a dataset by `layer`
   and `description`. Analysts should start with `warehouse-curated`.
2. Run its `how_to_query` example in DuckDB, e.g.
   `duckdb data/warehouse.duckdb "SELECT * FROM curated.wallets"`.

## Keeping it correct

- `scripts/generate_catalog.py` builds the catalog from a real pipeline run,
  with descriptions defined in `pipeline/catalog.py`.
- `scripts/validate_catalog.py` (CI) runs the pipeline on sample activity and
  fails if any real dataset is missing from the catalog, any catalogued dataset
  does not exist, any column or type differs, or any metadata field is empty.
  It also self-tests by removing an entry and confirming that is caught.

## Simulated vs production

| Here | Production equivalent |
|---|---|
| `catalog/catalog.json` in the repo | DataHub, Amundsen, OpenMetadata, Unity Catalog or AWS Glue Data Catalog |
| Validation in CI | Metadata ingested automatically from the warehouse and lake; ownership enforced by policy |
| `intended_consumers` field | Access control: analysts get `curated` only; `history` and the lake are restricted |
