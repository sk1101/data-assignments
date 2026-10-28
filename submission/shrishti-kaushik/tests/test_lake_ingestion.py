"""Lake ingestion tests: every change reaches the lake exactly once, even across
crashes, restarts and lost checkpoints. See docs/03_lake_and_checkpoint.md.
"""

import json

import pytest

from pipeline import checkpoint, lake
from pipeline.capture import install_cdc, read_changes
from pipeline.ingest import LakePaths, ingest_all, ingest_once
from pipeline.lake import lake_files, read_lake
from source.db import connect, create_schema
from tests.factories import seed


@pytest.fixture
def source():
    conn = connect()
    create_schema(conn)
    install_cdc(conn)
    seed(conn)  # 12 captured changes, lsn 1-12
    yield conn
    conn.close()


@pytest.fixture
def paths(tmp_path):
    return LakePaths.under(tmp_path)


def lake_lsns(paths):
    return [row["lsn"] for row in read_lake(paths.lake_dir)]


def source_lsns(conn):
    return [change.lsn for change in read_changes(conn)]


def json_or_none(text):
    return None if text is None else json.loads(text)


def crash_while_writing(table, path):
    """Simulates the process dying halfway through writing a file."""
    path.write_bytes(b"PAR1 half-written")
    raise OSError("simulated crash while writing")


def crash_before_writing(table, path):
    raise OSError("simulated crash before writing")


def crash_before_checkpoint(path, last_lsn):
    raise RuntimeError("simulated crash before saving the checkpoint")


# ── Happy path ───────────────────────────────────────────────────────────────


def test_every_change_reaches_the_lake_once(source, paths):
    ingest_all(source, paths)
    assert lake_lsns(paths) == source_lsns(source)


def test_lake_record_preserves_the_change(source, paths):
    changes = {change.lsn: change for change in read_changes(source)}
    ingest_all(source, paths)
    for row in read_lake(paths.lake_dir):
        change = changes[row["lsn"]]
        assert row["table_name"] == change.table
        assert row["op"] == change.op
        assert json.loads(row["pk"]) == change.pk
        assert json_or_none(row["before"]) == change.before
        assert json_or_none(row["after"]) == change.after
        assert row["captured_at"] == change.captured_at
        assert row["batch_id"] and row["ingested_at"]


def test_batches_are_named_by_contiguous_lsn_ranges(source, paths):
    ingest_all(source, paths, batch_size=5)
    assert [p.name for p in lake_files(paths.lake_dir)] == [
        "lsn_000000000001_000000000005.parquet",
        "lsn_000000000006_000000000010.parquet",
        "lsn_000000000011_000000000012.parquet",
    ]


def test_rerun_with_nothing_new_writes_nothing(source, paths):
    ingest_all(source, paths)
    files_before = lake_files(paths.lake_dir)
    assert ingest_all(source, paths) == []
    assert lake_files(paths.lake_dir) == files_before


def test_restart_continues_from_checkpoint(source, paths, tmp_path):
    ingest_all(source, paths)
    source.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    restarted = LakePaths.under(tmp_path)  # a new process, same storage
    results = ingest_all(source, restarted)
    assert [result.count for result in results] == [1]
    assert lake_lsns(paths) == source_lsns(source)


def test_deletes_reach_the_lake_with_the_deleted_row(source, paths):
    source.execute("DELETE FROM customers WHERE customer_id = 3")
    ingest_all(source, paths)
    last = read_lake(paths.lake_dir)[-1]
    assert (last["table_name"], last["op"]) == ("customers", "D")
    assert json.loads(last["before"])["phone"] == "+971500000003"
    assert last["after"] is None


def test_existing_lake_files_are_never_modified(source, paths):
    ingest_all(source, paths)
    originals = {p.name: p.read_bytes() for p in lake_files(paths.lake_dir)}
    source.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    ingest_all(source, paths)
    for path in lake_files(paths.lake_dir):
        if path.name in originals:
            assert path.read_bytes() == originals[path.name]


# ── Reliability: crashes and lost progress ──────────────────────────────────


def test_crash_while_writing_leaves_no_partial_batch(source, paths, monkeypatch):
    monkeypatch.setattr(lake, "_write_parquet", crash_while_writing)
    with pytest.raises(OSError):
        ingest_once(source, paths)
    assert lake_files(paths.lake_dir) == [], "a half-written file looks like a real batch"

    monkeypatch.undo()  # restart with a healthy process
    ingest_all(source, paths)
    assert lake_lsns(paths) == source_lsns(source)


def test_crash_after_write_before_checkpoint_causes_no_duplicates(source, paths, monkeypatch):
    monkeypatch.setattr(checkpoint, "save_checkpoint", crash_before_checkpoint)
    with pytest.raises(RuntimeError):
        ingest_once(source, paths)

    monkeypatch.undo()
    source.execute(
        "UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10"
    )  # arrives meanwhile
    ingest_all(source, paths)
    assert lake_lsns(paths) == source_lsns(source)


def test_lost_checkpoint_causes_no_duplicates(source, paths):
    ingest_all(source, paths)
    paths.checkpoint_file.unlink()
    source.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    ingest_all(source, paths)
    assert lake_lsns(paths) == source_lsns(source)


def test_rewound_checkpoint_causes_no_duplicates(source, paths):
    ingest_all(source, paths)
    checkpoint.save_checkpoint(paths.checkpoint_file, 3)  # e.g. restored from an old backup
    source.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    ingest_all(source, paths)
    assert lake_lsns(paths) == source_lsns(source)


def test_crash_with_pruning_loses_no_changes(source, paths, monkeypatch):
    all_lsns = source_lsns(source)
    monkeypatch.setattr(lake, "_write_parquet", crash_before_writing)
    with pytest.raises(OSError):
        ingest_once(source, paths, prune=True)

    monkeypatch.undo()
    ingest_all(source, paths, prune=True)
    assert lake_lsns(paths) == all_lsns, "changes were pruned before reaching the lake"


def test_pruning_removes_only_changes_already_in_the_lake(source, paths):
    ingest_all(source, paths, prune=True)
    assert read_changes(source) == []
    source.execute("UPDATE wallets SET balance_minor = 4000 WHERE wallet_id = 10")
    remaining = read_changes(source)
    assert len(remaining) == 1
    assert remaining[0].lsn > max(lake_lsns(paths))
