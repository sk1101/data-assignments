#!/usr/bin/env python3
"""Generate contracts/source_contract.json from source/schema.sql.

Run only when a source schema change has been agreed. Then REVIEW the diff
and commit it through a pull request (docs/04_schema_contract.md).

Usage: python scripts/generate_contract.py [version]
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.schema_guard import CONTRACT_PATH, describe_source, write_contract
from source.db import connect, create_schema


def main(version: int) -> int:
    conn = connect()
    create_schema(conn)
    write_contract(CONTRACT_PATH, describe_source(conn), version)
    print(f"Wrote {CONTRACT_PATH} (version {version}). Review the diff before committing.")
    return 0


if __name__ == "__main__":
    sys.exit(main(int(sys.argv[1]) if len(sys.argv) > 1 else 1))
