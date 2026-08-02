"""Data quality gate - roda depois de cada batch, em producao.

Diferente de `tests/`, que roda no CI sobre dados fixos, isto roda sobre os
dados reais a cada execucao. Checks BLOQUEANTES abortam o batch antes que a
particao seja publicada; WARNINGS viram alerta no CloudWatch mas nao param a
esteira, porque nem toda anomalia e um defeito.

    python -m gmv.quality --batch-date 2023-07-15
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from . import config, storage

BLOCKING, WARNING = "BLOCKING", "WARNING"
UNKNOWN_SLA_DAYS = 7
VOLUME_WINDOW_DAYS = 30
VOLUME_TOLERANCE = 5.0
MIN_BATCH_FOR_RATES = 10


class Result:
    def __init__(self):
        self.rows: list[tuple[str, str, bool, str]] = []

    def add(self, severity, name, ok, detail=""):
        self.rows.append((severity, name, ok, detail))
        flag = "PASS" if ok else ("FAIL" if severity == BLOCKING else "WARN")
        print(f"  {flag:5} [{severity:8}] {name}" + (f" - {detail}" if detail else ""))

    @property
    def blocking_failures(self):
        return [r for r in self.rows if not r[2] and r[0] == BLOCKING]


def run(gold: DataFrame, batch_date: date) -> Result:
    r = Result()
    batch = gold.where(F.col("transaction_date") == F.lit(batch_date))

    # --- integridade estrutural (bloqueante) -------------------------------
    total = gold.count()
    r.add(BLOCKING, "grao unico (purchase_id, version_number)",
          total == gold.select("purchase_id", "version_number").distinct().count(),
          f"{total} linhas")

    gaps = (gold.groupBy("purchase_id")
            .agg(F.max("version_number").alias("mx"), F.count("*").alias("n"))
            .where(F.col("mx") != F.col("n")).count())
    r.add(BLOCKING, "version_number sem lacunas", gaps == 0, f"{gaps} compras com lacuna")

    r.add(BLOCKING, "nenhuma versao anterior ao evento de origem",
          gold.where(F.col("transaction_date") < F.to_date(F.col("version_valid_from_ts"))).count() == 0)

    # --- coerencia da metrica (bloqueante) ---------------------------------
    r.add(BLOCKING, "gmv_amount nunca nulo ou negativo",
          gold.where(F.col("gmv_amount").isNull() | (F.col("gmv_amount") < 0)).count() == 0)

    r.add(BLOCKING, "nao elegivel implica gmv_amount = 0",
          gold.where(~F.col("is_gmv_eligible") & (F.col("gmv_amount") != 0)).count() == 0)

    r.add(BLOCKING, "elegivel implica release_date preenchido",
          gold.where(F.col("is_gmv_eligible") & F.col("release_date").isNull()).count() == 0)

    r.add(BLOCKING, "subsidiary nunca nulo",
          gold.where(F.col("subsidiary").isNull()).count() == 0)

    # --- imutabilidade (bloqueante) ----------------------------------------
    future = gold.where(F.col("transaction_date") > F.lit(batch_date)).count()
    r.add(BLOCKING, "nenhuma particao futura", future == 0, f"{future} linhas a frente do batch")

    # --- reconciliacao com o cabecalho (warning, premissa A3) --------------
    reconcilable = gold.where(F.col("purchase_gross_value").isNull() & F.col("is_complete"))
    r.add(WARNING, "todo registro completo tem valor monetario",
          reconcilable.count() == 0, f"{reconcilable.count()} sem valor")

    # --- SLA de completude (warning, premissa A6) --------------------------
    # Precisa olhar a versao VIGENTE, nao o historico: toda compra nasce
    # UNKNOWN e isso e esperado. O problema e continuar UNKNOWN hoje.
    w = Window.partitionBy("purchase_id").orderBy(F.col("version_number").desc())
    latest = (gold.withColumn("_rn", F.row_number().over(w))
              .where(F.col("_rn") == 1).drop("_rn"))
    first_seen = gold.groupBy("purchase_id").agg(F.min("transaction_date").alias("first_seen"))
    stale = (latest.where(F.col("subsidiary") == config.UNKNOWN_SUBSIDIARY)
             .join(first_seen, "purchase_id")
             .where(F.col("first_seen") < F.lit(batch_date - timedelta(days=UNKNOWN_SLA_DAYS)))
             .count())
    r.add(WARNING, f"nenhuma compra ainda UNKNOWN apos {UNKNOWN_SLA_DAYS} dias",
          stale == 0, f"{stale} compras")

    # --- anomalia de volume (warning) --------------------------------------
    n = batch.count()
    window = (gold.where(F.col("transaction_date").between(
                  F.lit(batch_date - timedelta(days=VOLUME_WINDOW_DAYS)), F.lit(batch_date)))
              .groupBy("transaction_date").count()
              .agg(F.avg("count").alias("m")).first())
    baseline = (window["m"] or 0) if window else 0
    spike = baseline > 0 and n > baseline * VOLUME_TOLERANCE
    r.add(WARNING, "volume de versoes dentro da faixa historica",
          not spike, f"{n} versoes (media {baseline:.1f})")

    # --- taxa de retificacao (warning) -------------------------------------
    # Uma taxa so tem significado com amostra: num batch de 1 versao, qualquer
    # retificacao vira 100% e o alerta viraria ruido que ninguem mais le.
    restatements = batch.where(F.col("version_type") == "RESTATEMENT").count()
    if n < MIN_BATCH_FOR_RATES:
        r.add(WARNING, "taxa de retificacao", True,
              f"amostra pequena ({n} versoes), nao avaliado")
    else:
        rate = restatements / n
        r.add(WARNING, "taxa de retificacao abaixo de 50%",
              rate < 0.5, f"{restatements}/{n} ({rate:.0%}) do batch")

    return r


def main() -> None:
    ap = argparse.ArgumentParser(description="Data quality gate")
    ap.add_argument("--batch-date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()
    batch_date = datetime.strptime(args.batch_date, "%Y-%m-%d").date()

    spark = storage.get_spark("gmv-quality")
    print(f"\nData quality - batch {batch_date}\n" + "-" * 66)
    result = run(storage.read(spark, config.GOLD_HISTORY), batch_date)

    failures = result.blocking_failures
    print("-" * 66)
    print(f"{sum(1 for x in result.rows if x[2])}/{len(result.rows)} checks ok"
          + (f" - {len(failures)} BLOQUEANTE(S)" if failures else ""))
    spark.stop()
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
