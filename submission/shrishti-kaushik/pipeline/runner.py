"""One pipeline cycle: schema guard → lake → warehouse → quality report."""

import logging
from dataclasses import dataclass
from pathlib import Path

from pipeline.ingest import LakePaths, ingest_all
from pipeline.warehouse import applied_lsn, apply_changes
from quality.checks import run_checks, write_report

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class CycleResult:
    changes_ingested: int
    changes_applied: int
    warehouse_lsn: int
    checks_failed: int


def run_cycle(
    src,
    wh,
    paths: LakePaths,
    contract: dict,
    report_file: Path,
    batch_size: int = 1000,
    prune: bool = False,
) -> CycleResult:
    """Raises SchemaContractViolation (before reading anything) if the source schema changed."""
    batches = ingest_all(src, paths, batch_size=batch_size, prune=prune, contract=contract)
    applied = apply_changes(wh, contract, paths.lake_dir)
    results = run_checks(wh, contract, paths.lake_dir)
    write_report(results, report_file)

    failed = [result for result in results if not result.passed]
    for result in failed:
        log.error(
            "Quality check failed: %s %s (%d rows)", result.rule, result.name, result.failing_rows
        )
    cycle = CycleResult(
        sum(batch.count for batch in batches), applied, applied_lsn(wh), len(failed)
    )
    log.info("Cycle complete: %s", cycle)
    return cycle
