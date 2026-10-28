"""One ingestion cycle: source _cdc_log → lake → checkpoint (docs/03)."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from pipeline import checkpoint
from pipeline.capture import CDC_LOG, read_changes
from pipeline.lake import write_batch


@dataclass(frozen=True)
class LakePaths:
    lake_dir: Path
    checkpoint_file: Path

    @classmethod
    def under(cls, root: Path) -> "LakePaths":
        return cls(root / "lake" / "changes", root / "state" / "checkpoint.json")


@dataclass(frozen=True)
class BatchResult:
    first_lsn: int
    last_lsn: int
    count: int


def ingest_once(
    conn: sqlite3.Connection, paths: LakePaths, batch_size: int = 1000, prune: bool = False
) -> BatchResult | None:
    """Move one batch of changes into the lake. Returns None when there is nothing new."""
    # BUG 2: trusts the checkpoint alone. If a crash happened after the lake
    # write but before the checkpoint save, this re-reads changes already stored.
    position = checkpoint.load_checkpoint(paths.checkpoint_file)

    changes = read_changes(conn, after_lsn=position, limit=batch_size)
    if not changes:
        return None

    if prune:
        # BUG 3: prunes the source log BEFORE the batch is safely in the lake.
        # A crash during the write would lose these changes for good.
        conn.execute(f"DELETE FROM {CDC_LOG} WHERE lsn <= ?", (changes[-1].lsn,))
        conn.commit()

    write_batch(paths.lake_dir, changes)
    checkpoint.save_checkpoint(paths.checkpoint_file, changes[-1].lsn)
    return BatchResult(changes[0].lsn, changes[-1].lsn, len(changes))


def ingest_all(
    conn: sqlite3.Connection, paths: LakePaths, batch_size: int = 1000, prune: bool = False
) -> list[BatchResult]:
    """Run ingestion cycles until the source has nothing new."""
    results = []
    while (result := ingest_once(conn, paths, batch_size, prune)) is not None:
        results.append(result)
    return results
