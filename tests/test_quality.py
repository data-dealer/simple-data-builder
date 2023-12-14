import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.sdb.quality import DeequQuality


@pytest.fixture(scope="module")
def df(spark):
    schema = StructType(
        [
            StructField("dv_hashkey_account", StringType(), True),
            StructField("dv_recsource", StringType(), True),
            StructField("dv_loaddts", StringType(), True),
            StructField("account_code", StringType(), True),
            StructField("count", IntegerType(), True),
        ]
    )
    data = [
        (0, "a1", "2023-01-03", "B", 15),
        (1, "a2", "2023-01-04", "C", 18),
        (2, "a1", "2023-01-05", "D", 20),
        (3, "a2", "2023-01-06", "E", 50),
    ]
    return spark.createDataFrame(data, schema)


def test_deequ_quality(table_meta, spark, df, logger):
    rs, summary = DeequQuality(
        spark=spark,
        target_table=table_meta.model["table_name"],
        check_suite=table_meta.deequ_quality_params,
        check_level="Warning",
    ).validate(df)
    rs.show()
    summary.show()

    assert rs.count() == 8
    assert summary.count() == 1
