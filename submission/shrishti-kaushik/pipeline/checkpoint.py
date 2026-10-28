"""Checkpoint: the last lsn known to be safely stored in the lake.

It is a fast-path hint only. The lake's own file names are the source of truth
for progress (docs/03), so a stale or missing checkpoint must never cause
duplicates or losses.
"""

import json
import os
from pathlib import Path

from pipeline.timeutil import utc_now


def load_checkpoint(path: Path) -> int:
    """Last lsn stored in the lake according to the checkpoint (0 if none yet)."""
    if not path.exists():
        return 0
    return int(json.loads(path.read_text())["last_lsn"])


def save_checkpoint(path: Path, last_lsn: int) -> None:
    """Atomically record progress: write a temp file, then rename it into place."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps({"last_lsn": last_lsn, "updated_at": utc_now()}))
    os.replace(tmp, path)
