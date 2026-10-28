"""Lake writer: append-only Parquet files, one immutable file per batch (docs/03)."""

import json
import os
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.capture import ChangeRecord
from pipeline.timeutil import utc_now

LAKE_SCHEMA = pa.schema(
    [
        ("lsn", pa.int64()),
        ("table_name", pa.string()),
        ("op", pa.string()),
        ("pk", pa.string()),
        ("before", pa.string()),
        ("after", pa.string()),
        ("captured_at", pa.string()),
        ("batch_id", pa.string()),
        ("ingested_at", pa.string()),
    ]
)

_BATCH_FILE = re.compile(r"^lsn_(\d{12})_(\d{12})\.parquet$")


def batch_id(first_lsn: int, last_lsn: int) -> str:
    return f"lsn_{first_lsn:012d}_{last_lsn:012d}"


def lake_files(lake_dir: Path) -> list[Path]:
    """Completed batch files, in lsn order. Anything else (e.g. *.tmp) is ignored."""
    if not lake_dir.exists():
        return []
    return sorted(p for p in lake_dir.iterdir() if _BATCH_FILE.match(p.name))


def highest_lsn_in_lake(lake_dir: Path) -> int:
    """Last lsn stored in the lake, read from the batch file names (0 if empty)."""
    files = lake_files(lake_dir)
    if not files:
        return 0
    return int(_BATCH_FILE.match(files[-1].name).group(2))


def _to_json(value: dict | None) -> str | None:
    return None if value is None else json.dumps(value, sort_keys=True)


def _write_parquet(table: pa.Table, path: Path) -> None:
    pq.write_table(table, path)


def write_batch(lake_dir: Path, changes: list[ChangeRecord]) -> Path:
    """Write one batch of changes as a single Parquet file named by its lsn range."""
    lake_dir.mkdir(parents=True, exist_ok=True)
    bid = batch_id(changes[0].lsn, changes[-1].lsn)
    ingested_at = utc_now()
    table = pa.Table.from_pylist(
        [
            {
                "lsn": c.lsn,
                "table_name": c.table,
                "op": c.op,
                "pk": _to_json(c.pk),
                "before": _to_json(c.before),
                "after": _to_json(c.after),
                "captured_at": c.captured_at,
                "batch_id": bid,
                "ingested_at": ingested_at,
            }
            for c in changes
        ],
        schema=LAKE_SCHEMA,
    )
    final_path = lake_dir / f"{bid}.parquet"
    if final_path.exists():
        # Same name means the same lsn range, i.e. the same changes, already stored.
        # Never overwrite: lake files are immutable.
        return final_path

    # Write to a temp name first, then rename. The rename is atomic, so readers
    # see either no file or the complete file, never a half-written one.
    tmp_path = lake_dir / f"{bid}.parquet.tmp"
    _write_parquet(table, tmp_path)
    os.replace(tmp_path, final_path)
    return final_path


def read_lake(lake_dir: Path, after_lsn: int = 0) -> list[dict]:
    """Lake records with lsn > after_lsn, in lsn order.

    Files whose whole range is at or below after_lsn are skipped by name, unopened.
    """
    rows: list[dict] = []
    for path in lake_files(lake_dir):
        last_lsn = int(_BATCH_FILE.match(path.name).group(2))
        if last_lsn <= after_lsn:
            continue
        rows.extend(row for row in pq.read_table(path).to_pylist() if row["lsn"] > after_lsn)
    return sorted(rows, key=lambda row: row["lsn"])
