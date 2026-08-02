"""Contrato da tabela: consumidores dependem deste schema."""
from gmv import config, storage
from gmv.history import FINAL_COLUMNS


def test_gold_exposes_exactly_the_documented_columns(spark, warehouse):
    actual = set(storage.read(spark, config.GOLD_HISTORY).columns)
    assert actual == set(FINAL_COLUMNS)


def test_money_columns_are_decimal_not_float(spark, warehouse):
    """Dinheiro em float e um bug esperando acontecer na conciliacao."""
    types = dict(storage.read(spark, config.GOLD_HISTORY).dtypes)
    assert types["gmv_amount"] == "decimal(18,2)"
    assert types["purchase_gross_value"] == "decimal(18,2)"


def test_partition_column_is_a_date(spark, warehouse):
    types = dict(storage.read(spark, config.GOLD_HISTORY).dtypes)
    assert types["transaction_date"] == "date"
