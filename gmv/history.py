from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import config, storage

FINAL_COLUMNS = [
    # grain
    "purchase_id", "version_number", "version_id", "version_type",
    # business time
    "order_date", "release_date", "gmv_date",
    # metric
    "purchase_gross_value", "gmv_amount", "is_gmv_eligible", "gmv_ineligibility_reason",
    # denormalised dimensions (requirement 6: no joins)
    "subsidiary", "purchase_status", "buyer_id", "producer_id", "product_id", "item_quantity",
    # completeness
    "is_complete", "missing_components",
    # lineage
    "version_valid_from_ts", "batch_id", "processing_date", "payload_hash", "change_reason",
    "src_purchase_event_ts", "src_product_item_event_ts", "src_extra_info_event_ts", "inserted_at",
    # system time / partition key (last: it is the partition column)
    "transaction_date",
]

_PREV_COLUMNS = ["purchase_id", "version_number", "payload_hash", "missing_components",
                 *config.PAYLOAD_FIELDS]


def history_before(spark: SparkSession, batch_date: date) -> DataFrame | None:
    h = storage.read_or_none(spark, config.GOLD_HISTORY)
    if h is None:
        return None
    return h.where(F.col("transaction_date") < F.lit(batch_date))


def latest_versions(history: DataFrame) -> DataFrame:
    """The version of each purchase that was current at the end of `history`."""
    w = Window.partitionBy("purchase_id").orderBy(F.col("version_number").desc())
    return history.withColumn("_rn", F.row_number().over(w)).where(F.col("_rn") == 1).drop("_rn")


def _change_reason(prev_missing_size, curr_missing_size):
    """Human-readable diff between the previous version and the candidate.

    We deliberately distinguish two causes that a naive SCD2 conflates:
      LATE_COMPONENT_ARRIVED - we learned something we did not know before
      *_RESTATED             - the source changed something it had told us
    Finance cares enormously about that difference during reconciliation.
    """
    reasons = [
        F.when(~F.col(field).eqNullSafe(F.col(f"prev_{field}")), F.lit(label))
        for field, label in config.CHANGE_REASONS.items()
    ]
    reasons.append(
        F.when(
            (F.col("prev_subsidiary") == F.lit(config.UNKNOWN_SUBSIDIARY))
            & (F.col("subsidiary") != F.lit(config.UNKNOWN_SUBSIDIARY)),
            F.lit("SUBSIDIARY_ARRIVED_LATE"),
        ).when(
            ~F.col("subsidiary").eqNullSafe(F.col("prev_subsidiary")),
            F.lit("SUBSIDIARY_RESTATED"),
        )
    )
    reasons.append(F.when(curr_missing_size < prev_missing_size, F.lit("LATE_COMPONENT_ARRIVED")))
    return F.array_compact(F.array(*reasons))


def build_new_versions(
    spark: SparkSession, candidates: DataFrame, previous: DataFrame | None,
    batch_date: date, batch_id: str,
) -> DataFrame:
    if previous is None:
        # Empty frame with exactly the right schema -> single code path below.
        previous = (
            candidates.select(*[c for c in _PREV_COLUMNS if c != "version_number"])
            .withColumn("version_number", F.lit(0))
            .limit(0)
        )
       
    previous = previous.select(*[F.col(c).alias(f"prev_{c}") for c in _PREV_COLUMNS])

    joined = candidates.join(
        previous, F.col("purchase_id") == F.col("prev_purchase_id"), "left"
    )

    is_new = F.col("prev_purchase_id").isNull()
    prev_missing = F.coalesce(F.size(F.col("prev_missing_components")), F.lit(99))
    curr_missing = F.size(F.col("missing_components"))

    changed = joined.where(
        is_new | ~F.col("payload_hash").eqNullSafe(F.col("prev_payload_hash"))
    )

    return (
        changed
        .withColumn("version_number", F.coalesce(F.col("prev_version_number"), F.lit(0)) + 1)
        .withColumn(
            "version_type",
            F.when(is_new, F.lit("INITIAL"))
            .when(curr_missing < prev_missing, F.lit("LATE_ARRIVAL"))
            .otherwise(F.lit("RESTATEMENT")),
        )
        .withColumn(
            "change_reason",
            F.when(is_new, F.array(F.lit("NEW")))
            .otherwise(_change_reason(prev_missing, curr_missing)),
        )
        .withColumn(
            "version_id",
            F.sha2(F.concat_ws("|", F.col("purchase_id"), F.col("version_number")), 256),
        )
        .withColumn("transaction_date", F.lit(batch_date).cast("date"))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("processing_date", F.current_date())
        .withColumn("inserted_at", F.current_timestamp())
        .select(*FINAL_COLUMNS)
    )


def load(spark: SparkSession, batch_date: date, batch_id: str, candidates: DataFrame) -> int:
    previous = history_before(spark, batch_date)
    previous_latest = latest_versions(previous) if previous is not None else None

    new_versions = build_new_versions(spark, candidates, previous_latest, batch_date, batch_id)
    new_versions = new_versions.cache()
    count = new_versions.count()

    if count:
        storage.append_partition(new_versions, config.GOLD_HISTORY)
    new_versions.unpersist()
    return count
