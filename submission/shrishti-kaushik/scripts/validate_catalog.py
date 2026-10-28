#!/usr/bin/env python3
"""CI catalog check (docs/07_catalog.md).

1. The committed catalog matches the datasets a real pipeline run produces.
2. Self-test: a catalog with an entry removed must be rejected.

Exit 0 when both hold, 1 otherwise.
"""

import copy
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.catalog import actual_datasets, load_catalog, validate_catalog
from pipeline.sample import build_sample_pipeline
from pipeline.schema_guard import load_contract


def main() -> int:
    contract = load_contract()
    catalog = load_catalog()
    with tempfile.TemporaryDirectory() as tmp:
        _, paths, wh = build_sample_pipeline(Path(tmp), contract)
        actual = actual_datasets(wh, paths.lake_dir)

    problems = validate_catalog(catalog, actual)
    if problems:
        print("✗ Catalog validation FAILED:")
        print("\n".join(f"  - {problem}" for problem in problems))
    else:
        print(f"✓ catalog lists all {len(actual)} datasets with complete, matching metadata")

    broken = copy.deepcopy(catalog)
    broken["datasets"].pop()
    caught = bool(validate_catalog(broken, actual))
    print("✓ self-test: a missing entry is caught" if caught else "✗ self-test FAILED")

    return 0 if not problems and caught else 1


if __name__ == "__main__":
    sys.exit(main())
