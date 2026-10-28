"""Catalog: discoverable metadata for every lake and warehouse dataset.
See docs/07_catalog.md.
"""

import json
from pathlib import Path

import duckdb

CATALOG_PATH = Path(__file__).resolve().parent.parent / "catalog" / "catalog.json"
LAKE_GLOB = "data/lake/changes/*.parquet"
WAREHOUSE_FILE = "data/warehouse.duckdb"
LAYERS = {"lake", "warehouse-curated", "warehouse-history"}

REQUIRED_FIELDS = [
    "name",
    "layer",
    "description",
    "location",
    "format",
    "owner",
    "intended_consumers",
    "update_cadence",
    "retention",
    "primary_key",
    "how_to_query",
    "columns",
]

TABLE_DESCRIPTIONS = {
    "customers": "People using the wallet app: identity, KYC status and account status.",
    "wallets": "One balance per customer per currency, in minor units (fils, cents, paise).",
    "transfers": "Money movements between two wallets, with their lifecycle status.",
    "ledger_entries": (
        "Double-entry debit/credit lines posted when a transfer completes or reverses."
    ),
    "transfer_status_events": "Audit trail of every transfer status change.",
}


# ── What really exists ───────────────────────────────────────────────────────


def _describe(conn: duckdb.DuckDBPyConnection, relation: str) -> list[dict]:
    rows = conn.execute(f"DESCRIBE {relation}").fetchall()
    return [{"name": row[0], "type": row[1]} for row in rows]


def actual_datasets(wh: duckdb.DuckDBPyConnection, lake_dir: Path) -> dict[str, list[dict]]:
    """The real columns of every lake and warehouse dataset."""
    datasets = {
        "lake.cdc_changes": _describe(wh, f"SELECT * FROM read_parquet('{lake_dir}/*.parquet')")
    }
    for schema in ("curated", "history"):
        tables = wh.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = ? ORDER BY table_name",
            [schema],
        ).fetchall()
        for (table,) in tables:
            datasets[f"warehouse.{schema}.{table}"] = _describe(wh, f"{schema}.{table}")
    return datasets


# ── Building and validating the catalog ──────────────────────────────────────


def _metadata(name: str, contract: dict) -> dict:
    if name == "lake.cdc_changes":
        return {
            "layer": "lake",
            "description": (
                "Every change captured from the source, append-only (raw layer). One row "
                "per change in lsn order; before/after are JSON images of the row."
            ),
            "location": LAKE_GLOB,
            "format": "parquet",
            "owner": "data-platform",
            "intended_consumers": ["data engineers (replay, rebuild, debugging)", "auditors"],
            "update_cadence": "every ingestion cycle",
            "retention": "forever; files are immutable",
            "primary_key": ["lsn"],
            "how_to_query": f"SELECT * FROM read_parquet('{LAKE_GLOB}') "
            "WHERE table_name = 'wallets' ORDER BY lsn",
        }

    _, schema, table = name.split(".")
    key = contract["tables"][table]["primary_key"]
    if schema == "curated":
        return {
            "layer": "warehouse-curated",
            "description": f"Current state. {TABLE_DESCRIPTIONS[table]}",
            "location": f"{WAREHOUSE_FILE} → curated.{table}",
            "format": "DuckDB view",
            "owner": "data-platform",
            "intended_consumers": ["analysts", "finance", "BI dashboards"],
            "update_cadence": "near real time: every pipeline cycle",
            "retention": f"current state only; history in warehouse.history.{table}",
            "primary_key": key,
            "how_to_query": f"SELECT * FROM curated.{table}",
        }
    return {
        "layer": "warehouse-history",
        "description": f"Every version of every row (SCD Type 2). {TABLE_DESCRIPTIONS[table]}",
        "location": f"{WAREHOUSE_FILE} → history.{table}",
        "format": "DuckDB table",
        "owner": "data-platform",
        "intended_consumers": ["auditors", "data engineers (restore, time travel)"],
        "update_cadence": "near real time: every pipeline cycle",
        "retention": "forever",
        "primary_key": key + ["_lsn"],
        "how_to_query": f"SELECT * FROM history.{table} WHERE {key[0]} = ? ORDER BY _lsn",
    }


def build_catalog(actual: dict[str, list[dict]], contract: dict) -> dict:
    return {
        "catalog_version": 1,
        "source_contract_version": contract["version"],
        "datasets": [
            {"name": name, **_metadata(name, contract), "columns": columns}
            for name, columns in sorted(actual.items())
        ],
    }


def load_catalog(path: Path = CATALOG_PATH) -> dict:
    return json.loads(path.read_text())


def write_catalog(path: Path, catalog: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False) + "\n")


def validate_catalog(catalog: dict, actual: dict[str, list[dict]]) -> list[str]:
    """Every way the catalog disagrees with reality (empty list = valid)."""
    problems: list[str] = []
    entries = {entry.get("name"): entry for entry in catalog.get("datasets", [])}

    for name in sorted(actual.keys() - entries.keys()):
        problems.append(f"{name}: exists but is not in the catalog")
    for name in sorted(entries.keys() - actual.keys()):
        problems.append(f"{name}: in the catalog but does not exist")

    for name in sorted(entries.keys() & actual.keys()):
        entry = entries[name]
        for field in REQUIRED_FIELDS:
            if not entry.get(field):
                problems.append(f"{name}: missing {field}")
        if entry.get("layer") not in LAYERS:
            problems.append(f"{name}: unknown layer {entry.get('layer')!r}")
        if entry.get("columns") != actual[name]:
            problems.append(f"{name}: columns differ from the real dataset")
    return problems
