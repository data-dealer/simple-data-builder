import json

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, IntegerType, LongType, MapType,
                               StringType, StructField, StructType)

from src.sdb.schema import (EvolutionConflictType, EvolutionMissingColumn,
                            evolution_handle)


@pytest.fixture(scope="module")
def df_origin(spark):
    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("code", StringType(), True),
            StructField("name", StringType(), True),
            StructField("count", LongType(), True),
            StructField("tuoi", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("map_data", MapType(StringType(), StringType()), True),
            StructField("array_data", ArrayType(StringType()), True),
        ]
    )
    data = [
        (1, "ix1", "John", 100, "x", "xy", {"name": "test0", "age": 1}, ["x", "y", "Z"])
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture(scope="module")
def df_stage(spark):
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", LongType(), True),
            StructField("gender", LongType(), True),
            StructField("map_data", StringType(), True),
            StructField("array_data", StringType(), True),
        ]
    )
    data = [
        ("2", "Jane", 30, 1, '{"name": "test2", "age": "2"}', '["a", "b"]'),
        ("2", "Bob", 16, 0, '{"age": "12"}', "a,b"),
    ]
    return spark.createDataFrame(data, schema)


def test_schema_pattern(df_origin, df_stage, logger):
    # logger.info("origin schema: " + df_origin._jdf.schema().treeString())
    # logger.info("stage schema: " + df_stage._jdf.schema().treeString())

    df_resolved_missing, conflict_missing, _ = EvolutionMissingColumn(
        df_origin, df_stage
    ).resolve_conflict()

    # logger.info("conflict_missing: " + str(conflict_missing))
    # df_resolved_missing.show(truncate=False)
    # logger.info("resolved missing schema: " + df_resolved_missing._jdf.schema().treeString())

    df_resolved_type, conflict_type, cannot_solved_type = EvolutionConflictType(
        df_origin, df_stage
    ).resolve_conflict()

    # logger.info("conflict_type: " + str(conflict_type))
    # logger.info(f"cannot_solved_type: {cannot_solved_type}")
    # df_resolved_type.show(truncate=False)
    # logger.info("resolved type schema: " + df_resolved_type._jdf.schema().treeString())

    assert len(df_resolved_missing.columns) == 9
    assert len(conflict_missing["EvolutionMissingColumnStage"]["fields"]) == 3
    assert df_resolved_type.select("map_data").schema.fields[0].dataType == MapType(
        StringType(), StringType()
    )
    assert df_resolved_type.select("array_data").schema.fields[0].dataType == ArrayType(
        StringType()
    )


def test_evolution_handle(df_origin, df_stage, logger):
    df_stage_resolved, conflict, cannot_solved = evolution_handle(df_origin, df_stage)
    # logger.info("conflict: " + str(conflict))
    # logger.info(f"cannot_solved_type: {cannot_solved}")
    # df_stage_resolved.show(truncate=False)
    # logger.info("resolved type schema: " + df_stage_resolved._jdf.schema().treeString())

    assert len(df_stage_resolved.columns) == 9
    assert len(conflict["EvolutionMissingColumnStage"]["fields"]) == 3
    assert df_stage_resolved.select("map_data").schema.fields[0].dataType == MapType(
        StringType(), StringType()
    )
    assert df_stage_resolved.select("array_data").schema.fields[
        0
    ].dataType == ArrayType(StringType())
