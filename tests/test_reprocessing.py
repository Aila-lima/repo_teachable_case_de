"""Reprocessamento: idempotencia, determinismo e a regressao do bug da Silver."""
from datetime import date

import pytest
from pyspark.sql import functions as F

from gmv import config, pipeline, seed, storage

BUSINESS_COLUMNS = [
    "purchase_id", "version_number", "version_type", "transaction_date",
    "order_date", "release_date", "gmv_date", "purchase_gross_value", "gmv_amount",
    "is_gmv_eligible", "subsidiary", "purchase_status", "buyer_id", "producer_id",
    "product_id", "item_quantity", "is_complete", "payload_hash",
]


def _fingerprint(df):
    rows = df.select(*BUSINESS_COLUMNS).orderBy("purchase_id", "version_number").collect()
    return [tuple(r) for r in rows]


def test_rerunning_a_closed_batch_is_a_noop(spark, scratch_warehouse):
    before = _fingerprint(storage.read(spark, config.GOLD_HISTORY))
    pipeline.run_batch(spark, date(2023, 2, 5))
    spark.catalog.clearCache()
    after = _fingerprint(storage.read(spark, config.GOLD_HISTORY))
    assert after == before


def test_replay_from_a_past_day_reproduces_everything(spark, scratch_warehouse):
    before = _fingerprint(storage.read(spark, config.GOLD_HISTORY))
    days = [d for d in seed.event_days(spark) if d >= date(2023, 1, 23)]
    pipeline.replay(spark, days)
    spark.catalog.clearCache()
    assert _fingerprint(storage.read(spark, config.GOLD_HISTORY)) == before


def test_reprocessing_the_past_does_not_leak_future_state(spark, scratch_warehouse):
    """REGRESSAO — o bug encontrado no desenvolvimento.

    A Silver guarda o estado mais recente por chave. Reprocessar 05/02 depois de
    julho ja ter sido incorporado fazia a montagem usar o estado de julho, e a
    versao 3 da compra 55 era reescrita com release_date de marco e valor 55,00
    — dados que nao existiam em 5 de fevereiro.

    A Gold era append-only e ainda assim produzia historia falsificada: o
    vazamento estava na camada de baixo. A correcao foi dar a Silver um rebuild
    point-in-time quando batch_date <= watermark.
    """
    pipeline.run_batch(spark, date(2023, 2, 5))
    spark.catalog.clearCache()

    v3 = (
        storage.read(spark, config.GOLD_HISTORY)
        .where((F.col("purchase_id") == 55) & (F.col("version_number") == 3))
        .first()
    )
    assert str(v3.release_date) == "2023-01-20", "vazou o release_date de julho"
    assert v3.gmv_amount == 50, "vazou a correcao de valor de julho"
    assert str(v3.gmv_date) == "2023-01-20"


def test_silver_switches_to_point_in_time_rebuild(spark, scratch_warehouse):
    """O guarda que impede o vazamento acima precisa disparar sozinho."""
    from gmv import silver

    assert silver.needs_rebuild(date(2023, 2, 5)) is True     # passado -> rebuild
    watermark = storage.read_watermark()
    assert silver.needs_rebuild(date(2024, 1, 1)) is False    # futuro -> incremental
    assert watermark is not None


@pytest.mark.slow
def test_full_replay_from_scratch_is_deterministic(spark, scratch_warehouse, tmp_path):
    """Reconstruir do zero tem que dar exatamente a mesma Gold.

    Marcado como `slow`: reprocessa os 12 dias de ingestao. Fora do ciclo rapido
    porque `make check` ja executa a mesma verificacao num subprocesso isolado,
    que e onde ela pertence - dentro de uma sessao Spark longa ela degrada.
    """
    before = _fingerprint(storage.read(spark, config.GOLD_HISTORY))

    config.WAREHOUSE = tmp_path / "rebuilt"
    seed.seed(spark)
    pipeline.replay(spark, seed.event_days(spark))
    spark.catalog.clearCache()

    assert _fingerprint(storage.read(spark, config.GOLD_HISTORY)) == before
