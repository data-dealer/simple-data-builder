from pyspark.sql.types import (ArrayType, IntegerType, MapType, StringType,
                               StructField, StructType)

from src.sdb.cdc_extract import TableTimeTravelCdc


def dummy_data(spark):
    data = [
        (0, "a", "2023-01-03", ["x", "y"], {"name": "test0", "age": "1"}),
        (12, "a", "2023-01-04", ["x", "y", "Z"], {"name": "test12", "age": "low"}),
        (1, "b", "2023-01-03", ["x", "y"], {"name": "test1", "age": "low"}),
        (2, "b", "2023-01-03", ["x", "y"], {"name": "test2", "age": "low"}),
        (2, "b", "2023-01-04", ["x", "y"], {"name": "test2", "age": "medium"}),
        (3, "c", "2023-01-03", ["x", "y"], {"name": "test3", "age": "old"}),
        (3, "d", "2023-01-04", ["x", "y"], {"name": "test3", "age": "medium"}),
        (4, "d", "2023-01-04", ["x", "y"], {"name": "test4", "age": "medium"}),
        (4, "d", "2023-01-03", None, {"name": "test4", "age": "medium"}),
    ]
    df = spark.createDataFrame(
        data=data,
        schema=StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("id2", StringType(), True),
                StructField("data_date", StringType(), True),
                StructField("test_array", ArrayType(StringType()), True),
                StructField("test_map", MapType(StringType(), StringType()), True),
            ]
        ),
    )
    return df


def dummy_data_non_his(spark):
    data = [
        (12, "a", "2023-01-04", ["x", "y", "Z"], {"name": "test12", "age": "low"}),
        (2, "b", "2023-01-04", ["x", "y"], {"name": "test2", "age": "medium"}),
        (3, "d", "2023-01-04", ["x", "y"], {"name": "test3", "age": "medium"}),
        (4, "d", "2023-01-04", ["x", "y"], {"name": "test4", "age": "medium"}),
    ]
    df = spark.createDataFrame(
        data=data,
        schema=StructType(
            [
                StructField("id", StringType(), True),
                StructField("id2", StringType(), True),
                StructField("data_date", StringType(), True),
                StructField("test_array", ArrayType(StringType()), True),
                StructField("test_map", MapType(StringType(), StringType()), True),
            ]
        ),
    )
    return df


def test_table_time_travel(spark, logger):
    df = dummy_data(spark)

    cdc_tbl = TableTimeTravelCdc(
        spark,
        "test_dataset",
        df=df,
        primary_cols=["id", "id2"],
        compare_cols=["test_map", "test_array"],
        date_partition_col="data_date",
    )
    rs_df = cdc_tbl.extract_cdc()
    rs = rs_df.collect()[0]

    rs_df.show()

    assert rs["cdc_count_events"]["I"] == 2
    assert rs["cdc_count_events"]["U"] == 2
    assert rs["cdc_count_events"]["D"] == 3
    assert rs["count_current_dataset"] == 4
    assert rs["count_previous_dataset"] == 5
    assert cdc_tbl.curr_date == "2023-01-04"
    assert cdc_tbl.prev_date == "2023-01-03"


def test_table_time_non_his(spark, logger):
    df = dummy_data_non_his(spark)

    cdc_tbl = TableTimeTravelCdc(
        spark,
        "test_dataset",
        df=df,
        primary_cols=["id", "id2"],
        compare_cols=["test_map", "test_array"],
        date_partition_col="data_date",
        # curr_date="2023-01-04",
    )
    rs_df = cdc_tbl.extract_cdc()
    rs = rs_df.collect()[0]
    rs_df.show()

    assert rs["cdc_count_events"]["I"] == 4
