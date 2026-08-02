"""Serving layer: registers the SQL views and runs the deliverable queries."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from . import config, storage

SQL_DIR = config.PROJECT_ROOT / "sql"


def register(spark: SparkSession) -> None:
    storage.read(spark, config.GOLD_HISTORY).createOrReplaceTempView(config.GOLD_HISTORY)
    spark.sql(read_sql("vw_purchase_gmv_current.sql"))


def read_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text()


def query(spark: SparkSession, filename: str, **params):
    sql = read_sql(filename)
    for k, v in params.items():
        sql = sql.replace(f"{{{{{k}}}}}", str(v))
    return spark.sql(sql)
