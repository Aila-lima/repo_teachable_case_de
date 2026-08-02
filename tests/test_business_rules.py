"""Regra de negocio: o que entra e o que nao entra no GMV."""
from pyspark.sql import functions as F


def _v(gold, purchase_id, version):
    return gold.where(
        (F.col("purchase_id") == purchase_id) & (F.col("version_number") == version)
    ).first()


def test_unreleased_purchase_is_not_eligible(gold):
    """56 v1 chegou sem release_date: existe na tabela, mas nao conta GMV."""
    row = _v(gold, 56, 1)
    assert row.is_gmv_eligible is False
    assert row.gmv_ineligibility_reason == "NOT_RELEASED"
    assert row.gmv_amount == 0


def test_refunded_purchase_leaves_gmv(gold):
    """69 v4 foi reembolsada: o valor sai do GMV sem nenhuma linha ser deletada."""
    assert _v(gold, 69, 3).gmv_amount == 2000
    row = _v(gold, 69, 4)
    assert row.is_gmv_eligible is False
    assert row.gmv_amount == 0


def test_ineligible_rows_always_have_zero_amount(gold):
    """Invariante global: se nao e elegivel, gmv_amount e zero."""
    assert gold.where(~F.col("is_gmv_eligible") & (F.col("gmv_amount") != 0)).count() == 0


def test_gmv_date_follows_release_date(gold):
    """Premissa A1, verificada em vez de assumida."""
    assert gold.where(~F.col("gmv_date").eqNullSafe(F.col("release_date"))).count() == 0


def test_subsidiary_is_never_null(gold):
    assert gold.where(F.col("subsidiary").isNull()).count() == 0


def test_unknown_subsidiary_is_transient(current):
    """UNKNOWN pode existir no historico, mas nunca na versao vigente."""
    assert current.where(F.col("subsidiary") == "UNKNOWN").count() == 0


def test_naive_sum_is_correct(current):
    """Requisito 6: SUM(gmv_amount) sem WHERE precisa dar o numero certo.

    55 -> 55,00   56 -> 2400,00   69 -> 0,00 (reembolsada)
    """
    total = current.agg(F.sum("gmv_amount")).first()[0]
    assert total == 2455
