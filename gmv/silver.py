"""Silver: per-key state compaction.

For each entity we keep exactly one row per business key: the latest event
observed so far. This is a *derived cache* - it can always be rebuilt from
Bronze - which is why it is allowed to be mutable while Gold is not.

Two execution paths, and the distinction matters:

* INCREMENTAL (the daily D-1 run). Read only the D-1 Bronze partition and fold
  it into the existing state. O(new events).

* POINT-IN-TIME REBUILD (any reprocessing of a day at or before the watermark).
  Recompute the state from Bronze with `transaction_date <= batch_date`.
  Without this, replaying an old day would assemble it using *today's* state
  and silently rewrite history with information that did not exist yet - the
  exact failure mode requirement 9 is guarding against.

Ordering is last-write-wins by `transaction_datetime`, so an event carrying an
older ingestion timestamp cannot overwrite fresher state (assumption A5).
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import config, storage

ENTITIES = {
    config.BRONZE_PURCHASE: (config.SILVER_PURCHASE, ["purchase_id"]),
    config.BRONZE_PRODUCT_ITEM: (config.SILVER_PRODUCT_ITEM, [config.ITEM_JOIN_KEY]),
    config.BRONZE_EXTRA_INFO: (config.SILVER_EXTRA_INFO, ["purchase_id"]),
}


def needs_rebuild(batch_date: date) -> bool:
    watermark = storage.read_watermark()
    return watermark is not None and batch_date <= watermark


def compact(spark: SparkSession, batch_date: date) -> str:
    mode = "REBUILD" if needs_rebuild(batch_date) else "INCREMENTAL"

    for bronze, (silver, keys) in ENTITIES.items():
        source = storage.read(spark, bronze)

        if mode == "REBUILD":
            combined = source.where(F.col("transaction_date") <= F.lit(batch_date))
        else:
            new_events = source.where(F.col("transaction_date") == F.lit(batch_date))
            if new_events.isEmpty():
                continue
            previous = storage.read_or_none(spark, silver)
            combined = new_events if previous is None else previous.unionByName(new_events)

        if combined.isEmpty():
            continue

        w = Window.partitionBy(*keys).orderBy(
            F.col("transaction_datetime").desc(), F.col("transaction_date").desc()
        )
        state = (
            combined.withColumn("_rn", F.row_number().over(w))
            .where(F.col("_rn") == 1)
            .drop("_rn")
        )
        storage.atomic_replace(state, silver)

    return mode
