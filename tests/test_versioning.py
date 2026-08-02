"""Versionamento: quando uma versao nasce, e por que."""
from pyspark.sql import functions as F


def test_grain_is_unique(gold):
    assert gold.count() == gold.select("purchase_id", "version_number").distinct().count()


def test_version_numbers_have_no_gaps(gold):
    gaps = (
        gold.groupBy("purchase_id")
        .agg(F.max("version_number").alias("mx"), F.count("*").alias("n"))
        .where(F.col("mx") != F.col("n"))
    )
    assert gaps.count() == 0


def test_expected_number_of_versions(gold):
    counts = {r.purchase_id: r.n for r in gold.groupBy("purchase_id").agg(F.count("*").alias("n")).collect()}
    assert counts == {55: 5, 56: 2, 69: 4}


def test_consecutive_versions_always_differ(gold):
    """Nenhuma versao nasce sem mudanca de conteudo: reenvio identico nao versiona."""
    from pyspark.sql.window import Window

    w = Window.partitionBy("purchase_id").orderBy("version_number")
    same = (
        gold.withColumn("prev_hash", F.lag("payload_hash").over(w))
        .where(F.col("payload_hash") == F.col("prev_hash"))
    )
    assert same.count() == 0


def test_late_component_is_classified_as_late_arrival(gold):
    """55 v2: o extra_info chegou depois. Aprendemos algo, nao foi retificacao."""
    row = gold.where((F.col("purchase_id") == 55) & (F.col("version_number") == 2)).first()
    assert row.version_type == "LATE_ARRIVAL"
    assert "SUBSIDIARY_ARRIVED_LATE" in row.change_reason
    assert "LATE_COMPONENT_ARRIVED" in row.change_reason


def test_source_correction_is_classified_as_restatement(gold):
    """69 v3: a origem trocou nacional por internacional. Isso e retificacao."""
    row = gold.where((F.col("purchase_id") == 69) & (F.col("version_number") == 3)).first()
    assert row.version_type == "RESTATEMENT"
    assert row.change_reason == ["SUBSIDIARY_RESTATED"]


def test_change_reason_explains_the_gmv_move(gold):
    """55 v5 e a versao que tira a compra de janeiro. O motivo tem que estar la."""
    row = gold.where((F.col("purchase_id") == 55) & (F.col("version_number") == 5)).first()
    assert "RELEASE_DATE_CHANGED" in row.change_reason
    assert str(row.gmv_date) == "2023-03-01"


def test_first_version_is_initial(gold):
    firsts = gold.where(F.col("version_number") == 1).select("version_type").distinct().collect()
    assert [r.version_type for r in firsts] == ["INITIAL"]
