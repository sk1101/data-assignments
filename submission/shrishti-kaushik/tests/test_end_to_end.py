"""End-to-end tests: the demo tells the whole story, and the pipeline (on its
own connection, like a real deployment) only ever sees committed changes.
"""

from pipeline.capture import install_cdc
from pipeline.ingest import LakePaths
from pipeline.runner import run_cycle
from pipeline.schema_guard import load_contract
from pipeline.warehouse import connect_warehouse, create_warehouse, current
from run import demo
from source.db import connect, create_schema
from source.sample_activity import run_sample_activity


def test_demo_runs_the_whole_story(tmp_path, capsys):
    assert demo(tmp_path / "demo") == 0
    output = capsys.readouterr().out
    assert "STOPPED" in output
    assert "lake files before: " in output


def test_pipeline_sees_only_committed_changes_across_connections(tmp_path):
    contract = load_contract()
    db_path = str(tmp_path / "source.db")
    app = connect(db_path)  # the application's connection
    create_schema(app)
    install_cdc(app)
    run_sample_activity(app)

    pipeline_src = connect(db_path)  # the pipeline's own connection
    wh = connect_warehouse()
    create_warehouse(wh, contract)
    paths = LakePaths.under(tmp_path)
    report = tmp_path / "quality_report.json"
    run_cycle(pipeline_src, wh, paths, contract, report)

    app.execute("UPDATE wallets SET balance_minor = balance_minor + 1 WHERE wallet_id = 30")
    in_flight = run_cycle(pipeline_src, wh, paths, contract, report)
    assert in_flight.changes_ingested == 0, "an uncommitted change was ingested"

    app.commit()
    committed = run_cycle(pipeline_src, wh, paths, contract, report)
    assert committed.changes_ingested == 1
    balances = {w["wallet_id"]: w["balance_minor"] for w in current(wh, contract, "wallets")}
    assert balances[30] == 5_001
