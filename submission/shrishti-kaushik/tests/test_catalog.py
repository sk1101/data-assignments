"""Catalog tests: the committed catalog matches the real datasets, covers both
layers, and validation rejects every kind of broken catalog (docs/07_catalog.md).
"""

import copy

import pytest

from pipeline.catalog import actual_datasets, load_catalog, validate_catalog
from pipeline.sample import build_sample_pipeline
from pipeline.schema_guard import load_contract


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    contract = load_contract()
    _, paths, wh = build_sample_pipeline(tmp_path_factory.mktemp("catalog"), contract)
    yield actual_datasets(wh, paths.lake_dir), contract
    wh.close()


def test_committed_catalog_matches_the_real_datasets(built):
    actual, _ = built
    assert validate_catalog(load_catalog(), actual) == []


def test_catalog_covers_the_lake_and_every_warehouse_table(built):
    _, contract = built
    names = {dataset["name"] for dataset in load_catalog()["datasets"]}
    expected = {"lake.cdc_changes"} | {
        f"warehouse.{schema}.{table}"
        for schema in ("curated", "history")
        for table in contract["tables"]
    }
    assert expected <= names


def test_catalog_records_the_contract_version(built):
    _, contract = built
    assert load_catalog()["source_contract_version"] == contract["version"]


BREAKAGES = [
    pytest.param(lambda c: c["datasets"].pop(), id="dataset_missing"),
    pytest.param(lambda c: c["datasets"][0].pop("owner"), id="owner_missing"),
    pytest.param(lambda c: c["datasets"][0]["columns"][0].update(type="VARCHAR"), id="wrong_type"),
    pytest.param(
        lambda c: c["datasets"].append({**c["datasets"][0], "name": "warehouse.curated.refunds"}),
        id="dataset_does_not_exist",
    ),
    pytest.param(lambda c: c["datasets"][0].update(layer="gold"), id="unknown_layer"),
]


@pytest.mark.parametrize("break_catalog", BREAKAGES)
def test_validation_rejects_a_broken_catalog(built, break_catalog):
    actual, _ = built
    catalog = copy.deepcopy(load_catalog())
    break_catalog(catalog)
    assert validate_catalog(catalog, actual), "a broken catalog was accepted"
