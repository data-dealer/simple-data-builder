from __future__ import annotations

from datetime import datetime
from typing import List

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (ArrayType, BooleanType, DateType, LongType,
                               MapType, StringType, StructField, StructType)


class CdcExtract:
    ACTION_COL_NAME = "action"
    ACTION_DELETE = "D"
    ACTION_UPDATE = "U"
    ACTION_INSERT = "I"
    ACTION_IGNORE = "_"
    CURR_PREFIX = "CURR"
    PREV_PREFIX = "PREV"
    HASH_KEY = "HASH_KEY"
    HASH_DIFF = "HASH_DIFF"

    def __init__(
        self,
        curr_df: DataFrame,
        prev_df: DataFrame,
        primary_cols: list[str],
        compare_cols: list[str],
    ):
        """Change data capture for 2 datasets

        Args:
            curr_df (DataFrame): current dataset
            prev_df (DataFrame): previous dataset
            primary_cols (List[str]): unique keys
            compare_cols (List[str]):
                columns for detailed data change detection
        """
        self.curr_df = curr_df
        self.prev_df = prev_df
        self.primary_cols = primary_cols
        self.compare_cols = compare_cols

        self._validate_params()

    def _validate_params(self):
        _unique_list = list(dict.fromkeys(self.compare_cols))
        _cnt = len(self.compare_cols)

        if len(self.primary_cols) == 0:
            self.primary_cols = _unique_list
            print("*primary_cols must be defined")
        if _cnt == 0:
            raise Exception("*compare_cols must be defined")
        if len(_unique_list) != _cnt:
            raise Exception("*compare_cols are not unique")

    @staticmethod
    def _parse_hash_cols(
        dataframe: DataFrame,
        primary_cols: list[str],
        compare_cols: list[str],
        hash_key: str = "HASH_KEY",
        hash_diff: str = "HASH_DIFF",
    ) -> DataFrame:
        def parse_hash_key(cols: list[str]):
            if len(cols) > 1:
                return f.md5(f.concat_ws("", *[f.col(c).cast("string") for c in cols]))
            else:
                return f.md5(f.col(cols[0]).cast("string"))

        return dataframe.withColumn(hash_key, parse_hash_key(primary_cols)).withColumn(
            hash_diff, parse_hash_key(compare_cols)
        )

    @staticmethod
    def _parse_insert_cons(hash_key: str, curr_prefix: str, prev_prefix: str) -> str:
        return f"{curr_prefix}.{hash_key} is not null \
                and {prev_prefix}.{hash_key} is null"

    @staticmethod
    def _parse_delete_cons(hash_key: str, curr_prefix: str, prev_prefix: str) -> str:
        return (
            f"{curr_prefix}.{hash_key} is null and {prev_prefix}.{hash_key} is not null"
        )

    @staticmethod
    def _parse_update_cons(
        hash_key: str, hash_diff: str, curr_prefix: str, prev_prefix: str
    ) -> str:
        all_conditions = [
            f"{curr_prefix}.{hash_key} is not null and {prev_prefix}.{hash_key} is not null",
            f"{curr_prefix}.{hash_diff} <> {prev_prefix}.{hash_diff}",
        ]
        return " and ".join(all_conditions)

    def _parse_result_select_statement(self) -> list:
        select_statements = []
        for col in [
            self.HASH_KEY,
            self.HASH_DIFF,
            *self.primary_cols,
            *self.compare_cols,
        ]:
            select_statements.append(
                f.expr(
                    f"""
                            case when {self.ACTION_COL_NAME} = '{self.ACTION_DELETE}'
                            then {self.PREV_PREFIX}.{col}
                            else {self.CURR_PREFIX}.{col}
                            end as {col}
                        """
                )
            )
        select_statements.append(self.ACTION_COL_NAME)
        return select_statements

    def extract_cdc_detail(self) -> DataFrame:
        """extract detailed records in dataset with cdc

        Returns:
            DataFrame:
        """
        curr_df_hashed = self._parse_hash_cols(
            dataframe=self.curr_df,
            primary_cols=self.primary_cols,
            compare_cols=self.compare_cols,
            hash_key=self.HASH_KEY,
            hash_diff=self.HASH_DIFF,
        )
        prev_df_hashed = self._parse_hash_cols(
            dataframe=self.prev_df,
            primary_cols=self.primary_cols,
            compare_cols=self.compare_cols,
            hash_key=self.HASH_KEY,
            hash_diff=self.HASH_DIFF,
        )
        merged_df = curr_df_hashed.alias(self.CURR_PREFIX).join(
            prev_df_hashed.alias(self.PREV_PREFIX),
            f.col(f"{self.CURR_PREFIX}.{self.HASH_KEY}")
            == f.col(f"{self.PREV_PREFIX}.{self.HASH_KEY}"),
            how="full_outer",
        )
        merged_df = merged_df.withColumn(
            self.ACTION_COL_NAME,
            f.when(
                f.expr(
                    self._parse_insert_cons(
                        self.HASH_KEY, self.CURR_PREFIX, self.PREV_PREFIX
                    )
                ),
                self.ACTION_INSERT,
            )
            .when(
                f.expr(
                    self._parse_delete_cons(
                        self.HASH_KEY, self.CURR_PREFIX, self.PREV_PREFIX
                    )
                ),
                self.ACTION_DELETE,
            )
            .when(
                f.expr(
                    self._parse_update_cons(
                        self.HASH_KEY,
                        self.HASH_DIFF,
                        self.CURR_PREFIX,
                        self.PREV_PREFIX,
                    )
                ),
                self.ACTION_UPDATE,
            )
            .otherwise(self.ACTION_IGNORE),
        )

        result_df = merged_df.select(self._parse_result_select_statement()).filter(
            f.col(self.ACTION_COL_NAME) != self.ACTION_IGNORE
        )

        return result_df

    def extract_cdc_summary(self, result_df: DataFrame):
        _df = result_df.groupby(self.ACTION_COL_NAME).count()
        cdc_dict = {
            row.asDict()[self.ACTION_COL_NAME]: row.asDict()["count"]
            for row in _df.collect()
        }

        return cdc_dict


class TableTimeTravelCdc:
    def __init__(
        self,
        spark: SparkSession,
        dataset_name: str,
        df: DataFrame,
        primary_cols: list[str],
        compare_cols: list[str],
        date_partition_col: str,
        curr_date: str = None,
    ):
        self.spark = spark
        self.dataset_name = dataset_name
        self.df = df
        self.primary_cols = primary_cols
        self.compare_cols = compare_cols
        self.date_partition_col = date_partition_col

        if curr_date:
            self.curr_date = curr_date
        else:
            self.curr_date = self._get_data_date(self.df, self.date_partition_col)
        self.prev_date = self._get_data_date(
            self.df, self.date_partition_col, self.curr_date
        )

        print(self.curr_date, self.prev_date)

    @staticmethod
    def _get_data_date(
        df: DataFrame, date_partition_col: str, lt_date: str = None
    ) -> str:
        if lt_date:
            df = df.where(f.col(date_partition_col) < lt_date)
        items = (
            df.select(date_partition_col)
            .orderBy(date_partition_col, ascending=False)
            .limit(1)
            .collect()
        )
        if len(items) > 0:
            return items[0][date_partition_col]
        else:
            return None

    def extract_cdc(self) -> DataFrame:
        curr_df = self.df.select(
            *self.primary_cols, *self.compare_cols, self.date_partition_col
        ).where(f.col(self.date_partition_col) == self.curr_date)
        prev_df = self.df.select(
            *self.primary_cols, *self.compare_cols, self.date_partition_col
        ).where(f.col(self.date_partition_col) == self.prev_date)

        cdc_extract = CdcExtract(
            curr_df=curr_df,
            prev_df=prev_df,
            primary_cols=self.primary_cols,
            compare_cols=self.compare_cols,
        )

        cdc_detail = cdc_extract.extract_cdc_detail()
        cdc_summary = cdc_extract.extract_cdc_summary(cdc_detail)
        count_curr_dataset = curr_df.count()
        count_prev_dataset = prev_df.count()
        status_cdc = cdc_detail.count() > 0  # need checked

        curr_keys_unique = (
            curr_df.count() == curr_df.select(*self.primary_cols).distinct().count()
        )
        prev_keys_unique = (
            prev_df.count() == prev_df.select(*self.primary_cols).distinct().count()
        )

        rs_dict = {
            "dataset_name": self.dataset_name,
            "changed_vs_previous": status_cdc,
            "current_datadate": datetime.strptime(self.curr_date, "%Y-%m-%d").date()
            if self.curr_date
            else None,
            "previous_datadate": datetime.strptime(self.prev_date, "%Y-%m-%d").date()
            if self.prev_date
            else None,
            "count_current_dataset": count_curr_dataset,
            "count_previous_dataset": count_prev_dataset,
            "current_keys_unique": curr_keys_unique,
            "previous_keys_unique": prev_keys_unique,
            "cdc_count_events": cdc_summary,
            "primary_cols": self.primary_cols,
            "compare_cols": self.compare_cols,
            "date_partition_col": self.date_partition_col,
        }

        return self.spark.createDataFrame(
            data=[rs_dict],
            schema=StructType(
                [
                    StructField("dataset_name", StringType(), True),
                    StructField("changed_vs_previous", BooleanType(), True),
                    StructField("current_datadate", DateType(), True),
                    StructField("previous_datadate", DateType(), True),
                    StructField("count_current_dataset", LongType(), True),
                    StructField("count_previous_dataset", LongType(), True),
                    StructField("current_keys_unique", BooleanType(), True),
                    StructField("previous_keys_unique", BooleanType(), True),
                    StructField(
                        "cdc_count_events", MapType(StringType(), LongType(), True)
                    ),
                    StructField("primary_cols", ArrayType(StringType(), True)),
                    StructField("compare_cols", ArrayType(StringType(), True)),
                    StructField("date_partition_col", StringType(), True),
                ]
            ),
        ).withColumn("created_time", f.current_timestamp())
