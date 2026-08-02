"""Invariant tests - the claims this model makes, asserted in code.
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
from datetime import date

from pyspark.sql import functions as F

from . import config, pipeline, seed, storage

BUSINESS_COLUMNS = [
    "purchase_id", "version_number", "version_type", "transaction_date",
    "order_date", "release_date", "gmv_date", "purchase_gross_value", "gmv_amount",
    "is_gmv_eligible", "subsidiary", "purchase_status", "buyer_id", "producer_id",
    "product_id", "item_quantity", "is_complete", "payload_hash",
]

_results: list[tuple[bool, str]] = []


def check(condition: bool, description: str) -> None:
    _results.append((condition, description))
    print(f"  {'PASS' if condition else 'FAIL'}  {description}")


def fingerprint(df) -> str:
    """Content hash of the Gold table, ignoring wall-clock metadata."""
    rows = df.select(*BUSINESS_COLUMNS).orderBy("purchase_id", "version_number").collect()
    return hashlib.sha256("|".join(str(tuple(r)) for r in rows).encode()).hexdigest()


def main() -> None:
    spark = storage.get_spark("gmv-checks")
    gold = storage.read(spark, config.GOLD_HISTORY)

    print("\n[1] Grain and version integrity")
    total = gold.count()
    distinct = gold.select("purchase_id", "version_number").distinct().count()
    check(total == distinct, f"(purchase_id, version_number) is unique ({total} rows)")

    gaps = (
        gold.groupBy("purchase_id")
        .agg(F.max("version_number").alias("mx"), F.count("*").alias("n"))
        .where(F.col("mx") != F.col("n"))
        .count()
    )
    check(gaps == 0, "version numbers are contiguous starting at 1 (no gaps)")

    print("\n[2] Monotonicity of history (as-of queries are stable)")
    asof_mar = gold.where(F.col("transaction_date") <= F.lit(date(2023, 3, 31)))
    asof_dec = gold.where(F.col("transaction_date") <= F.lit(date(2023, 12, 31)))
    check(
        asof_mar.count() < asof_dec.count()
        and asof_mar.subtract(asof_dec.select(asof_mar.columns)).count() == 0,
        "the as-of-March snapshot is a strict prefix of the as-of-December one",
    )

    print("\n[3] Every version is anchored to its ingestion day (partitioning is meaningful)")
    misplaced = gold.where(
        F.col("transaction_date") < F.to_date(F.col("version_valid_from_ts"))
    ).count()
    check(misplaced == 0, "transaction_date is never earlier than the source event it reflects")

    print("\n[4] Immutability under reprocessing (requirements 3 and 9)")
    before = fingerprint(gold)
    replay_days = [d for d in seed.event_days(spark) if d >= date(2023, 2, 5)]
    pipeline.replay(spark, replay_days)                   # reprocess a closed day forward
    spark.catalog.clearCache()                            # drop stale file listings
    after = fingerprint(storage.read(spark, config.GOLD_HISTORY))
    check(before == after, "reprocessing from a closed day reproduces every row unchanged")

    print("\n[5] Full-replay determinism (backfills are reproducible)")
    alt = "/tmp/gmv_replay_check"
    env = {**os.environ, "GMV_WAREHOUSE": alt}
    subprocess.run(
        [sys.executable, "-m", "gmv.pipeline", "--reset", "--seed", "--replay-all"],
        env=env, cwd=str(config.PROJECT_ROOT), check=True, capture_output=True,
    )
    replayed = spark.read.format(config.TABLE_FORMAT).load(f"{alt}/{config.GOLD_HISTORY}")
    check(
        fingerprint(replayed) == before,
        "replaying the pipeline from scratch reproduces Gold byte-for-byte",
    )

    failed = [d for ok, d in _results if not ok]
    print("\n" + "=" * 70)
    print(f"{len(_results) - len(failed)}/{len(_results)} checks passed")
    spark.stop()
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
