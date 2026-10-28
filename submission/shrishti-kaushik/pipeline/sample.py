"""Run the whole pipeline on sample activity: used by CI scripts, tests and the demo."""

from pathlib import Path

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths, ingest_all
from pipeline.warehouse import apply_changes, connect_warehouse, create_warehouse
from source.db import connect, create_schema
from source.sample_activity import run_sample_activity


def build_sample_pipeline(root: Path, contract: dict):
    """Source with sample activity → lake under `root` → in-memory warehouse."""
    src = connect()
    create_schema(src)
    install_cdc(src)
    run_sample_activity(src)
    paths = LakePaths.under(root)
    ingest_all(src, paths, contract=contract)
    wh = connect_warehouse()
    create_warehouse(wh, contract)
    apply_changes(wh, contract, paths.lake_dir)
    return src, paths, wh
