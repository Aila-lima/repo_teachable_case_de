from __future__ import annotations

import argparse
import shutil
from datetime import date, datetime

from . import assemble, config, history, seed, silver, storage


def run_batch(spark, batch_date: date) -> int:
    batch_id = f"gmv_daily__{batch_date.isoformat()}"
    mode = silver.compact(spark, batch_date)
    candidates = assemble.build_candidates(spark, batch_date)
    inserted = history.load(spark, batch_date, batch_id, candidates)
    storage.write_watermark(batch_date)
    print(f"  [{batch_date}] {inserted} new version(s)  (silver: {mode.lower()})")
    return inserted


def replay(spark, days) -> int:
    return sum(run_batch(spark, d) for d in days)


def main() -> None:
    ap = argparse.ArgumentParser(description="Bitemporal GMV pipeline")
    ap.add_argument("--reset", action="store_true", help="wipe the warehouse")
    ap.add_argument("--seed", action="store_true", help="(re)generate Bronze CDC events")
    ap.add_argument("--replay-all", action="store_true", help="replay every ingestion day in order")
    ap.add_argument("--batch-date", help="run a single D-1 batch (YYYY-MM-DD)")
    ap.add_argument("--replay-from", help="reprocess this day and every later one (YYYY-MM-DD)")
    args = ap.parse_args()

    if args.reset:
        shutil.rmtree(config.WAREHOUSE, ignore_errors=True)
        print(f"warehouse cleared: {config.WAREHOUSE}")

    spark = storage.get_spark()

    if args.seed or not storage.table_exists(config.BRONZE_PURCHASE):
        seed.seed(spark)
        print("bronze seeded")

    if args.batch_date:
        run_batch(spark, datetime.strptime(args.batch_date, "%Y-%m-%d").date())
    elif args.replay_from:
        start = datetime.strptime(args.replay_from, "%Y-%m-%d").date()
        days = [d for d in seed.event_days(spark) if d >= start]
        print(f"replaying {len(days)} day(s) from {start}")
        replay(spark, days)
    elif args.replay_all:
        days = seed.event_days(spark)
        print(f"replaying {len(days)} ingestion day(s)")
        total = replay(spark, days)
        print(f"done: {total} version(s) in {config.GOLD_HISTORY}")

    spark.stop()


if __name__ == "__main__":
    main()
