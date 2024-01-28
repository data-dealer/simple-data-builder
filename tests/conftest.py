import logging

import pytest
from pyspark.sql import SparkSession

from src.sdb.meta import TableMeta


@pytest.fixture(scope="session")
def logger():
    return logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark():
    print("init spark fixture")
    spark = (
        SparkSession.builder.appName("pytest")
        # .config("spark.jars", "/opt/workspace/jars/deequ-2.0.3-spark-3.3.jar")
        .getOrCreate()
    )
    return spark
    # yield spark
    # spark.sparkContext.stop()


meta_txt = """
version: 1.0.0

model:
  table_name: hub_account
  database_name: maxenv-t2_db_silver
  data_location: s3://maxenv-t2-dev-silver/iceberg/hub_account/data/
  checkpoint_location: s3://maxenv-t2-dev-silver/iceberg/hub_account/checkpoint
  partition_by: null
  data_format: iceberg
  columns:
    - name: key
      type: string
      tests:
        - check: expect_column_values_to_be_unique
          kwargs:
            result_format: BASIC
        - check: expect_column_values_to_be_in_set
          kwargs:
            value_set: ['x', 'y', 'z'] # [1, 2, 3]
      description: key
    - name: dv_hashkey_account
      type: string
      tests:
        - check: isComplete
        - check: isUnique
      description: dv hashkey

    - name: dv_recsource
      type: string
      tests:
        - check: isComplete
        - check: isContainedIn
          params:
            - ["b1", "a2"]
      description: dv columns

    - name: dv_loaddts
      type: timestamp
      description: dv columns

    - name: account_code
      type: string
      tests:
        - check: isComplete
        - check: isUnique

    - name: count
      type: integer
      tests:
        - check: isComplete
        - check: hasMax
          params:
            - "lambda x: x == 20"

depends:
  - table_name: gaccount
    database_name: maxenv-t2_db_bronze
  - table_name: laccount
    database_name: maxenv-t2_db_bronze

sla:
  - streaming

reconciles:
  - sources:
      - bronze.gaccount
    targets:
      - silver.hub_account
    source_sql: select COUNT(DISTINCT id) as cnt_distinct from source_table
    target_sql: select count(*) as cnt_distinct from target_table

  - sources:
      - bronze.sat_gaccount
    targets:
      - silver.hub_account
    source_sql: select count(*) as cnt from source_table
    target_sql: select count(*) as cnt from target_table
"""


@pytest.fixture(scope="session")
def table_meta():
    table_meta = TableMeta(from_text=meta_txt)

    return table_meta
