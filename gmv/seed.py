"""Seeds the Bronze layer with the CDC events from the case statement.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

from . import config, storage


def _yyyymm(d: date | None) -> int | None:
    return None if d is None else d.year * 100 + d.month


PURCHASE_EVENTS = [
    # (ts, purchase_id, buyer_id, prod_item_id, order_date, release_date, producer_id, status)
    ("2023-01-20 22:00:00", 55, 15947, 5, "2023-01-20", "2023-01-20", 852852, "APROVADA"),      # [CASE]
    ("2023-01-26 00:01:00", 56, 369798, 746520, "2023-01-25", None, 963963, "INICIADA"),        # [CASE]
    ("2023-02-05 10:00:00", 55, 160001, 5, "2023-01-20", "2023-01-20", 852852, "APROVADA"),     # [CASE] re-sent, buyer restated
    ("2023-02-10 11:00:00", 56, 369798, 746520, "2023-01-25", "2023-02-10", 963963, "APROVADA"),# [EXTRA] payment captured late
    ("2023-02-26 03:00:00", 69, 160001, 18, "2023-02-26", "2023-02-28", 96967, "APROVADA"),     # [CASE]
    ("2023-07-15 09:00:00", 55, 160001, 5, "2023-01-20", "2023-03-01", 852852, "APROVADA"),     # [CASE] release_date restated
    ("2023-08-10 08:00:00", 69, 160001, 18, "2023-02-26", "2023-02-28", 96967, "REEMBOLSADA"),  # [EXTRA] refund
]

PRODUCT_ITEM_EVENTS = [
    # (ts, purchase_id, prod_item_id, product_id, item_quantity, purchase_value)
    ("2023-01-20 22:02:00", 55, 5, 696969, 10, 50.00),       # [CASE]
    ("2023-01-25 23:59:59", 56, 746520, 808080, 120, 2400.00),  # [CASE] arrives BEFORE its purchase event
    ("2023-02-26 03:00:00", 69, 18, 373737, 2, 2000.00),     # [CASE]
    ("2023-07-12 09:00:00", 55, 5, 696969, 10, 55.00),       # [CASE] value correction, 6 months later
]

EXTRA_INFO_EVENTS = [
    # (ts, purchase_id, subsidiary)
    ("2023-01-23 00:05:00", 55, "nacional"),        # [CASE] late dimension
    ("2023-01-25 23:59:59", 56, "internacional"),   # [CASE]
    ("2023-02-28 01:10:00", 69, "nacional"),        # [CASE]
    ("2023-03-12 07:00:00", 69, "internacional"),   # [CASE] dimension restated
]

_PURCHASE_SCHEMA = StructType([
    StructField("purchase_id", LongType()),
    StructField("buyer_id", LongType()),
    StructField("prod_item_id", LongType()),
    StructField("order_date", DateType()),
    StructField("release_date", DateType()),
    StructField("producer_id", LongType()),
    StructField("purchase_partition", LongType()),
    StructField("prod_item_partition", LongType()),
    StructField("purchase_total_value", DoubleType()),
    StructField("purchase_status", StringType()),
    StructField("transaction_datetime", TimestampType()),
    StructField("transaction_date", DateType()),
])

_ITEM_SCHEMA = StructType([
    StructField("purchase_id", LongType()),
    StructField("prod_item_id", LongType()),
    StructField("prod_item_partition", LongType()),
    StructField("product_id", LongType()),
    StructField("item_quantity", IntegerType()),
    StructField("purchase_value", DoubleType()),
    StructField("transaction_datetime", TimestampType()),
    StructField("transaction_date", DateType()),
])

_EXTRA_SCHEMA = StructType([
    StructField("purchase_id", LongType()),
    StructField("purchase_partition", LongType()),
    StructField("subsidiary", StringType()),
    StructField("transaction_datetime", TimestampType()),
    StructField("transaction_date", DateType()),
])


def _ts(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _d(s: str | None) -> date | None:
    return None if s is None else datetime.strptime(s, "%Y-%m-%d").date()


def seed(spark: SparkSession) -> None:
    purchases = []
    for ts, pid, buyer, item_id, odate, rdate, prod, status in PURCHASE_EVENTS:
        t = _ts(ts)
        od = _d(odate)
        purchases.append((pid, buyer, item_id, od, _d(rdate), prod,
                          _yyyymm(od), _yyyymm(od), None, status, t, t.date()))

    items = []
    for ts, pid, item_id, product_id, qty, value in PRODUCT_ITEM_EVENTS:
        t = _ts(ts)
        items.append((pid, item_id, _yyyymm(t.date()), product_id, qty, value, t, t.date()))

    extras = []
    for ts, pid, subsidiary in EXTRA_INFO_EVENTS:
        t = _ts(ts)
        extras.append((pid, _yyyymm(t.date()), subsidiary, t, t.date()))

    storage.overwrite(spark.createDataFrame(purchases, _PURCHASE_SCHEMA),
                      config.BRONZE_PURCHASE, partition_by="transaction_date")
    storage.overwrite(spark.createDataFrame(items, _ITEM_SCHEMA),
                      config.BRONZE_PRODUCT_ITEM, partition_by="transaction_date")
    storage.overwrite(spark.createDataFrame(extras, _EXTRA_SCHEMA),
                      config.BRONZE_EXTRA_INFO, partition_by="transaction_date")


def event_days(spark: SparkSession) -> list[date]:
    """Every ingestion day that carries at least one event, chronologically.

    A production scheduler runs daily; days without events are no-ops. Replaying
    only the days that matter keeps the demo fast without changing semantics.
    """
    days: set[date] = set()
    for t in (config.BRONZE_PURCHASE, config.BRONZE_PRODUCT_ITEM, config.BRONZE_EXTRA_INFO):
        rows = storage.read(spark, t).select("transaction_date").distinct().collect()
        days.update(r["transaction_date"] for r in rows)
    return sorted(days)
