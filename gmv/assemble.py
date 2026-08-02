"""Assembly: turn three independent CDC streams into one candidate fact row.

The `purchase` event is the spine: no purchase event, no fact row. Line items
and dimensional attributes are LEFT joined, so a purchase becomes visible as
soon as it is known, flagged as incomplete, and is enriched by later versions.
That is deliberate - the table must record what we *knew at the time*, not a
retroactively perfect picture.
"""
from __future__ import annotations

from datetime import date
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config, storage

MONEY = "decimal(18,2)"


def affected_purchase_ids(spark: SparkSession, batch_date: date) -> DataFrame:
    """Purchases touched by any event ingested on `batch_date`.

    This is what keeps the batch incremental: we never rescan history, only the
    D-1 partition of each Bronze table.
    """
    parts = []
    for table in (config.BRONZE_PURCHASE, config.BRONZE_EXTRA_INFO):
        parts.append(
            storage.read(spark, table)
            .where(F.col("transaction_date") == F.lit(batch_date))
            .select("purchase_id")
        )

    items = storage.read(spark, config.BRONZE_PRODUCT_ITEM).where(
        F.col("transaction_date") == F.lit(batch_date)
    )
    if config.ITEM_JOIN_KEY == "purchase_id":
        parts.append(items.select("purchase_id"))
    else:
        # Item events do not carry purchase_id: resolve it through Silver.
        # If the purchase is not known yet, the id is picked up later, when the
        # purchase event itself arrives.
        purchases = storage.read_or_none(spark, config.SILVER_PURCHASE)
        if purchases is not None:
            parts.append(
                items.select("prod_item_id")
                .join(purchases.select("prod_item_id", "purchase_id"), "prod_item_id")
                .select("purchase_id")
            )

    return reduce(DataFrame.unionByName, parts).distinct()


def build_candidates(spark: SparkSession, batch_date: date) -> DataFrame:
    ids = affected_purchase_ids(spark, batch_date)

    purchases = storage.read(spark, config.SILVER_PURCHASE).select(
        "purchase_id", "buyer_id", "prod_item_id", "order_date", "release_date",
        "producer_id", "purchase_status",
        F.col("transaction_datetime").alias("src_purchase_event_ts"),
    )

    item_cols = [
        "product_id", "item_quantity",
        F.col("purchase_value").cast(MONEY).alias("purchase_gross_value"),
        F.col("transaction_datetime").alias("src_product_item_event_ts"),
    ]
    items = storage.read(spark, config.SILVER_PRODUCT_ITEM).select(
        config.ITEM_JOIN_KEY, *item_cols
    )

    extras = storage.read_or_none(spark, config.SILVER_EXTRA_INFO)
    if extras is None:
        extras = spark.createDataFrame([], "purchase_id bigint, subsidiary string, src_extra_info_event_ts timestamp")
    else:
        extras = extras.select(
            "purchase_id", "subsidiary",
            F.col("transaction_datetime").alias("src_extra_info_event_ts"),
        )

    df = (
        purchases.join(ids, "purchase_id", "left_semi")
        .join(items, config.ITEM_JOIN_KEY, "left")
        .join(extras, "purchase_id", "left")
    )

    # --- business rules ----------------------------------------------------
    released = F.col("release_date").isNotNull()
    cancelled = F.col("purchase_status").isin(config.CANCELLED_STATUSES)

    df = (
        df
        # Business time: GMV is recognised when the payment is captured.
        .withColumn("gmv_date", F.col("release_date"))
        .withColumn("is_gmv_eligible", released & ~cancelled)
        .withColumn(
            "gmv_ineligibility_reason",
            F.when(cancelled, F.upper(F.col("purchase_status")))
            .when(~released, F.lit("NOT_RELEASED"))
            .otherwise(F.lit(None).cast("string")),
        )
        # Pre-applied metric: SUM(gmv_amount) is correct with no WHERE clause,
        # which is what requirement 6 (non-expert SQL users) really asks for.
        .withColumn(
            "gmv_amount",
            F.when(F.col("is_gmv_eligible"), F.coalesce(F.col("purchase_gross_value"), F.lit(0)))
            .otherwise(F.lit(0))
            .cast(MONEY),
        )
        .withColumn("subsidiary", F.coalesce(F.col("subsidiary"), F.lit(config.UNKNOWN_SUBSIDIARY)))
        .withColumn(
            "missing_components",
            F.array_compact(F.array(
                F.when(F.col("src_product_item_event_ts").isNull(), F.lit("product_item")),
                F.when(F.col("src_extra_info_event_ts").isNull(), F.lit("purchase_extra_info")),
            )),
        )
        .withColumn("is_complete", F.size("missing_components") == 0)
        .withColumn(
            "version_valid_from_ts",
            F.greatest("src_purchase_event_ts", "src_product_item_event_ts", "src_extra_info_event_ts"),
        )
    )

    # Fingerprint of the business payload only: a re-sent identical event
    # produces the same hash and therefore no new version.
    payload = F.concat_ws(
        "|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in config.PAYLOAD_FIELDS]
    )
    return df.withColumn("payload_hash", F.sha2(payload, 256))
