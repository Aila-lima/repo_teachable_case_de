"""DELIVERABLE 3 + 4: prints sample rows and the analytical queries.

    python -m gmv.report
"""
from __future__ import annotations

from . import analytics, config, storage

SEP = "=" * 100


def section(title: str) -> None:
    print(f"\n{SEP}\n{title}\n{SEP}")


def main() -> None:
    spark = storage.get_spark("gmv-report")
    analytics.register(spark)

    section("1. fct_purchase_gmv_history - historico completo de versoes (append-only)")
    (
        spark.table(config.GOLD_HISTORY)
        .selectExpr(
            "purchase_id", "version_number AS v", "transaction_date AS ingested_on",
            "version_type", "subsidiary", "gmv_date", "gmv_amount",
            "is_gmv_eligible AS eligible", "purchase_status AS status",
            "is_complete AS complete", "change_reason",
        )
        .orderBy("purchase_id", "version_number")
        .show(50, truncate=False)
    )

    section("2. vw_purchase_gmv_current - uma linha por compra, sem necessidade de joins")
    spark.table("vw_purchase_gmv_current").selectExpr(
        "purchase_id", "version_number AS v", "subsidiary", "gmv_date",
        "gmv_amount", "purchase_status AS status", "gmv_ineligibility_reason AS why_not"
    ).orderBy("purchase_id").show(truncate=False)

    section("3. ENTREGAVEL 4 - GMV diario por subsidiary (verdade vigente)")
    analytics.query(spark, "gmv_daily_by_subsidiary.sql").show(truncate=False)

    section("4. REQUISITO 4 - consultas as of: janeiro/2023 visto de tres pontos no tempo")
    for as_of in ("2023-01-31", "2023-03-31", "2023-12-31"):
        print(f"\n--- GMV como era conhecido em {as_of} ---")
        (
            analytics.query(spark, "gmv_daily_by_subsidiary_asof.sql", as_of=as_of)
            .where("date BETWEEN DATE '2023-01-01' AND DATE '2023-01-31'")
            .show(truncate=False)
        )

    section("5. Linhagem diaria / conciliacao - o que mudou, quando e por que")
    spark.sql(
        """
        SELECT transaction_date AS ingestion_day, batch_id, purchase_id,
               version_number AS v, version_type, change_reason,
               gmv_date, gmv_amount, subsidiary
        FROM fct_purchase_gmv_history
        ORDER BY transaction_date, purchase_id
        """
    ).show(50, truncate=False)

    section("6. Auditoria de retificacao - o delta de GMV entre duas datas, explicado")
    spark.sql(
        """
        WITH before AS (
            SELECT * EXCEPT (rn) FROM (
                SELECT h.*, ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY version_number DESC) rn
                FROM fct_purchase_gmv_history h WHERE transaction_date <= DATE '2023-03-31') WHERE rn = 1
        ), after AS (
            SELECT * EXCEPT (rn) FROM (
                SELECT h.*, ROW_NUMBER() OVER (PARTITION BY purchase_id ORDER BY version_number DESC) rn
                FROM fct_purchase_gmv_history h) WHERE rn = 1
        )
        SELECT COALESCE(b.purchase_id, a.purchase_id) AS purchase_id,
               b.gmv_date AS gmv_date_before, a.gmv_date AS gmv_date_after,
               b.subsidiary AS subsidiary_before, a.subsidiary AS subsidiary_after,
               COALESCE(b.gmv_amount, 0) AS gmv_before,
               COALESCE(a.gmv_amount, 0) AS gmv_after,
               COALESCE(a.gmv_amount, 0) - COALESCE(b.gmv_amount, 0) AS delta,
               a.change_reason AS latest_reason
        FROM before b FULL OUTER JOIN after a USING (purchase_id)
        WHERE NOT (b.gmv_amount <=> a.gmv_amount AND b.gmv_date <=> a.gmv_date
                   AND b.subsidiary <=> a.subsidiary)
        ORDER BY purchase_id
        """
    ).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
