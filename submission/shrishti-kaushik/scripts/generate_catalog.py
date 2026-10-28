#!/usr/bin/env python3
"""Generate catalog/catalog.json from a real pipeline run. Review the diff, then commit.

Usage: python scripts/generate_catalog.py
"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.catalog import CATALOG_PATH, actual_datasets, build_catalog, write_catalog
from pipeline.sample import build_sample_pipeline
from pipeline.schema_guard import load_contract


def main() -> int:
    contract = load_contract()
    with tempfile.TemporaryDirectory() as tmp:
        _, paths, wh = build_sample_pipeline(Path(tmp), contract)
        catalog = build_catalog(actual_datasets(wh, paths.lake_dir), contract)
    write_catalog(CATALOG_PATH, catalog)
    print(f"Wrote {CATALOG_PATH} ({len(catalog['datasets'])} datasets). Review before committing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
