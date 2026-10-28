"""One ingestion cycle: source _cdc_log → lake → checkpoint (docs/03)."""

import sqlite3
from dataclasses import dataclass
from pathlib import Path

from pipeline import checkpoint
from pipeline.capture import CDC_LOG, read_changes
from pipeline.lake import highest_lsn_in_lake, write_batch
from pipeline.schema_guard import enforce_contract, load_contract


@dataclass(frozen=True)
class LakePaths:
    lake_dir: Path
    checkpoint_file: Path
    alert_file: Path

    @classmethod
    def under(cls, root: Path) -> "LakePaths":
        return cls(
            root / "lake" / "changes",
            root / "state" / "checkpoint.json",
            root / "state" / "schema_alert.json",
        )


@dataclass(frozen=True)
class BatchResult:
    first_lsn: int
    last_lsn: int
    count: int


def ingest_once(
    conn: sqlite3.Connection,
    paths: LakePaths,
    batch_size: int = 1000,
    prune: bool = False,
    contract: dict | None = None,
) -> BatchResult | None:
    """Move one batch of changes into the lake. Returns None when there is nothing new.

    Raises SchemaContractViolation, before reading anything, if the source schema
    no longer matches the contract.
    """
    # Stop the line first: never read changes under a schema we don't recognise.
    enforce_contract(conn, contract or load_contract(), paths.alert_file)

    # The lake is the source of truth for progress; the checkpoint is a hint.
    # Taking the max means a stale, lost or rewound checkpoint cannot cause duplicates.
    position = max(
        checkpoint.load_checkpoint(paths.checkpoint_file),
        highest_lsn_in_lake(paths.lake_dir),
    )

    changes = read_changes(conn, after_lsn=position, limit=batch_size)
    if not changes:
        return None

    write_batch(paths.lake_dir, changes)
    checkpoint.save_checkpoint(paths.checkpoint_file, changes[-1].lsn)

    if prune:
        # Only after the batch is durable in the lake AND the checkpoint is saved.
        # A crash before this point leaves the source log untouched.
        conn.execute(f"DELETE FROM {CDC_LOG} WHERE lsn <= ?", (changes[-1].lsn,))
        conn.commit()
    return BatchResult(changes[0].lsn, changes[-1].lsn, len(changes))


def ingest_all(
    conn: sqlite3.Connection,
    paths: LakePaths,
    batch_size: int = 1000,
    prune: bool = False,
    contract: dict | None = None,
) -> list[BatchResult]:
    """Run ingestion cycles until the source has nothing new."""
    results = []
    while (result := ingest_once(conn, paths, batch_size, prune)) is not None:
        results.append(result)
    return results
