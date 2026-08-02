from __future__ import annotations

import shutil

import pytest

from gmv import config, pipeline, seed, storage


@pytest.fixture(scope="session")
def spark():
    session = storage.get_spark("gmv-tests")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def warehouse(spark, tmp_path_factory):
    """Seed Bronze and replay every ingestion day into a throwaway warehouse."""
    config.WAREHOUSE = tmp_path_factory.mktemp("warehouse")
    seed.seed(spark)
    pipeline.replay(spark, seed.event_days(spark))
    return config.WAREHOUSE


@pytest.fixture(scope="session")
def gold(spark, warehouse):
    return storage.read(spark, config.GOLD_HISTORY).cache()


@pytest.fixture(scope="session")
def current(spark, gold):
    """The read-time 'current version' projection, as the serving view defines it."""
    gold.createOrReplaceTempView(config.GOLD_HISTORY)
    spark.sql((config.PROJECT_ROOT / "sql" / "vw_purchase_gmv_current.sql").read_text())
    return spark.table("vw_purchase_gmv_current").cache()


@pytest.fixture
def scratch_warehouse(spark, warehouse, tmp_path):
    """An isolated copy of the built warehouse, for tests that reprocess data."""
    original = config.WAREHOUSE
    scratch = tmp_path / "warehouse"
    shutil.copytree(original, scratch)
    config.WAREHOUSE = scratch
    spark.catalog.clearCache()
    yield scratch
    config.WAREHOUSE = original
    spark.catalog.clearCache()


def as_of(spark, cutoff: str):
    """GMV by subsidiary as it was known on `cutoff` — the requirement-4 query."""
    sql = (config.PROJECT_ROOT / "sql" / "gmv_daily_by_subsidiary_asof.sql").read_text()
    return spark.sql(sql.replace("{{as_of}}", cutoff))
