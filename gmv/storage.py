"""Spark session + thin storage abstraction.

The Gold table is append-only, so plain partitioned Parquet is already correct.
Delta/Iceberg is the production target for atomic commits, schema evolution,
MERGE on Silver and OPTIMIZE/Z-ORDER - swapping GMV_TABLE_FORMAT is enough.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from . import config


def get_spark(app_name: str = "gmv-bitemporal") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if config.TABLE_FORMAT == "delta":
        builder = (
            builder.config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def table_path(name: str) -> str:
    return str(config.WAREHOUSE / name)


def table_exists(name: str) -> bool:
    p = config.WAREHOUSE / name
    return p.exists() and any(p.iterdir())


def read(spark: SparkSession, name: str) -> DataFrame:
    return spark.read.format(config.TABLE_FORMAT).load(table_path(name))


def read_or_none(spark: SparkSession, name: str):
    return read(spark, name) if table_exists(name) else None


def overwrite(df: DataFrame, name: str, partition_by: str | None = None) -> None:
    w = df.write.format(config.TABLE_FORMAT).mode("overwrite")
    if partition_by:
        w = w.partitionBy(partition_by)
    w.save(table_path(name))


def append_partition(df: DataFrame, name: str, partition_by: str = "transaction_date") -> None:
    """Idempotent batch write.

    Dynamic partition overwrite replaces only the partitions present in `df`.
    Combined with `history_before()` - which ignores rows of the batch's own
    day - re-running a batch is a no-op instead of duplicating versions.
    Closed partitions are never touched.
    """
    (
        df.write.format(config.TABLE_FORMAT)
        .mode("overwrite")
        .partitionBy(partition_by)
        .save(table_path(name))
    )


def atomic_replace(df: DataFrame, name: str) -> None:
    """Replace a table whose own data is an input of `df`.

    Spark cannot overwrite a path it is reading from, so we materialise to a
    temp location and swap. In production this whole function is a single
    Delta/Iceberg MERGE, which is both atomic and incremental.
    """
    import shutil

    tmp = f"{table_path(name)}__tmp"
    df.write.format(config.TABLE_FORMAT).mode("overwrite").save(tmp)
    target = table_path(name)
    shutil.rmtree(target, ignore_errors=True)
    shutil.move(tmp, target)


# --- Pipeline watermark ----------------------------------------------------
# Tracks the last ingestion day folded into Silver. It is what lets the
# pipeline notice that it is being asked to reprocess the past and switch from
# the incremental path to a point-in-time rebuild.

def _watermark_file():
    return config.WAREHOUSE / "_watermark.json"


def read_watermark():
    import json
    from datetime import date as _date

    f = _watermark_file()
    if not f.exists():
        return None
    return _date.fromisoformat(json.loads(f.read_text())["last_batch_date"])


def write_watermark(value) -> None:
    import json

    config.WAREHOUSE.mkdir(parents=True, exist_ok=True)
    _watermark_file().write_text(json.dumps({"last_batch_date": value.isoformat()}))
