import hashlib
from datetime import date
from pathlib import Path

from pyspark.sql import functions as F

from gmv import config
from tests.conftest import as_of


def _january(df):
    return df.where(F.col("date").between(date(2023, 1, 1), date(2023, 1, 31)))


def test_january_as_known_in_march(spark, gold):
    """GMV de janeiro visto em 31/03: 50,00 em nacional."""
    gold.createOrReplaceTempView(config.GOLD_HISTORY)
    rows = _january(as_of(spark, "2023-03-31")).collect()
    assert len(rows) == 1
    assert rows[0].subsidiary == "nacional"
    assert rows[0].gmv == 50


def test_january_as_known_today_is_empty(spark, gold):
    """Hoje janeiro esta vazio: a compra 55 migrou para marco."""
    gold.createOrReplaceTempView(config.GOLD_HISTORY)
    assert _january(as_of(spark, "2023-12-31")).count() == 0


def test_the_value_moved_rather_than_vanished(spark, gold):
    """O GMV nao sumiu: reapareceu em 01/03 com o valor corrigido."""
    gold.createOrReplaceTempView(config.GOLD_HISTORY)
    row = (
        as_of(spark, "2023-12-31")
        .where(F.col("date") == date(2023, 3, 1))
        .first()
    )
    assert row.subsidiary == "nacional"
    assert row.gmv == 55


def test_as_of_snapshots_are_monotonic(gold):
    """Um snapshot mais antigo e sempre prefixo estrito de um mais novo.

    E o que garante que uma consulta 'as of' nunca muda de resposta.
    """
    early = gold.where(F.col("transaction_date") <= date(2023, 3, 31))
    late = gold.where(F.col("transaction_date") <= date(2023, 12, 31))
    assert early.count() < late.count()
    assert early.subtract(late.select(early.columns)).count() == 0


def test_no_version_predates_its_source_event(gold):
    """A particao tem que refletir quando o dado chegou, nao quando aconteceu."""
    bad = gold.where(F.col("transaction_date") < F.to_date(F.col("version_valid_from_ts")))
    assert bad.count() == 0


def test_business_time_and_system_time_are_independent(gold):
    """Uma versao escrita em julho pode carregar gmv_date de janeiro.

    E exatamente o caso de correcao retroativa sem reescrever particao.
    """
    row = gold.where(
        (F.col("purchase_id") == 55) & (F.col("transaction_date") == date(2023, 7, 12))
    ).first()
    assert str(row.gmv_date) == "2023-01-20"
    assert row.gmv_amount == 55


def _partition_fingerprint(warehouse: Path, day: str) -> str:
    part = warehouse / config.GOLD_HISTORY / f"transaction_date={day}"
    digest = hashlib.sha256()
    for f in sorted(part.glob("*.parquet")):
        digest.update(f.read_bytes())
    return digest.hexdigest()


def test_closed_partition_is_never_rewritten(spark, scratch_warehouse):
    """Reprocessar fevereiro em diante nao toca um unico byte de janeiro."""
    from gmv import pipeline, seed

    before = _partition_fingerprint(scratch_warehouse, "2023-01-20")
    days = [d for d in seed.event_days(spark) if d >= date(2023, 2, 5)]
    pipeline.replay(spark, days)
    assert _partition_fingerprint(scratch_warehouse, "2023-01-20") == before

def test_ingestion_timestamps_are_timezone_stable(gold):
    row = gold.where(
        (F.col("purchase_id") == 55) & (F.col("version_number") == 1)
    ).first()
    assert str(row.version_valid_from_ts) == "2023-01-20 22:02:00"
    assert str(row.transaction_date) == "2023-01-20"
