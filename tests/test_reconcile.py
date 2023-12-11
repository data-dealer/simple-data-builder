from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
    ArrayType,
    IntegerType
)
import pytest
import json

from src.sdb.reconcile import ReconcilitionSuite, SqlCountPair


@pytest.fixture(scope="module")
def df(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("id2", StringType(), True),
        StructField("data_date", StringType(), True),
        StructField("test_array", ArrayType(StringType()), True),
        StructField("test_map", MapType(StringType(), StringType()), True),
    ])
    data = [
        (0, "a", "2023-01-03", ["x", "y"], {"name": "test0", "age": "1"}),
        (12, "a", "2023-01-04", ["x", "y", "Z"],
         {"name": "test12", "age": "low"}),
        (1, "b", "2023-01-03", ["x", "y"], {"name": "test1", "age": "low"}),
        (2, "b", "2023-01-03", ["x", "y"], {"name": "test2", "age": "low"}),
        (2, "b", "2023-01-04", ["x", "y"], {"name": "test2", "age": "medium"}),
        (3, "c", "2023-01-03", ["x", "y"], {"name": "test3", "age": "old"}),
        (3, "d", "2023-01-04", ["x", "y"], {"name": "test3", "age": "medium"}),
        (4, "d", "2023-01-04", ["x", "y"], {"name": "test4", "age": "medium"}),
        (4, "d", "2023-01-03", None, {"name": "test4", "age": "medium"}),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def reconcile_suites(df):
    src_tbl_name = "source_table"
    tar_tbl_name = "target_table"
    df.createOrReplaceTempView(src_tbl_name)
    df.createOrReplaceTempView(tar_tbl_name)

    reconcile_suites = [
        {
            "sources": [
                "bronze.gaccount"
            ],
            "targets": [
                "silver.hub_account"
            ],
            "source_sql": "select COUNT(DISTINCT id) as cnt_distinct from source_table",
            "target_sql": "select count(*) as cnt_distinct from target_table"
        },
        {
            "sources": [
                "bronze.sat_gaccount"
            ],
            "targets": [
                "silver.hub_account"
            ],
            "source_sql": "select count(*) as cnt from source_table",
            "target_sql": "select count(*) as cnt from target_table"
        }
    ]
    return reconcile_suites


def test_sql_count_pair(spark, reconcile_suites, logger):

    # reconcile_suites = table_meta.reconciles
    logger.info("reconcile_suites: %s" %
                (json.dumps(reconcile_suites, indent=2)))

    rer = ReconcilitionSuite(spark, SqlCountPair, suite=reconcile_suites)
    rs_json, summary_json, status = rer.validate(output_format="json")
    rs_df, summary_df, _ = rer.validate(output_format="dataframe")
    logger.info("reconcile_results: %s" % (json.dumps(rs_json, indent=2)))
    logger.info("summary_json: %s" % (json.dumps(summary_json, indent=2)))

    logger.info(rs_df.show())
    logger.info(summary_df.show())

    assert type(rs_json) == list
    assert status == False
