from __future__ import annotations
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, FloatType, ArrayType, LongType
from jinja2 import Template
import uuid
from datetime import datetime


class ReconcilitionPair(ABC):
    """
    """

    def __init__(self, spark: SparkSession, sources: list, targets: list):
        self.spark = spark
        self.sources = sources
        self.targets = targets
        self.status = None


    def validate(self) -> None:
        """
        The template method
        """
        source_result = self.perpare_source()
        target_result = self.perpare_target()
        # self.status = self.compare(source_result, target_result)
        return self.compare(source_result, target_result)

    @abstractmethod
    def perpare_source(self):
        pass

    @abstractmethod
    def perpare_target(self):
        pass

    @abstractmethod
    def compare(self, source , taget) -> bool:
        pass


class SqlCountPair(ReconcilitionPair):

    def __init__(
        self,
        spark: SparkSession,
        sources: list,
        targets: list,
        source_sql: str,
        target_sql: str,
        jinja_data: dict = {"run_date": "2023-02-23"},
    ):
        """
        {
        "sources": ['stream_glisting'],
        "targets": ["hub_listing", "sat_glisting", "link_ald"],
        "source_sql": '''
        select cnt as cnt_hub_listing, cnt as cnt_sat_glisting, cnt as cnt_link_ald
        from (
        select COUNT(DISTINCT id) as cnt
        from iceberg_catalog.`maxenv-t2_db_bronze`.glisting
        )
        ''',
        "target_sql": '''
        select
        (select count(*) from iceberg_catalog.`maxenv-t2_db_silver`.hub_listing where dv_recsource = 'source_g') as cnt_hub_listing,
        (select count(*) from iceberg_catalog.`maxenv-t2_db_silver`.sat_glisting) as cnt_sat_glisting,
        (select count(*) from iceberg_catalog.`maxenv-t2_db_silver`.link_account_listing_district where dv_recsource = 'source_g') as cnt_link_ald
        '''
        }
        """
        super().__init__(spark, sources, targets)
        self.jinja_data = jinja_data
        self.source_sql = source_sql
        self.target_sql = target_sql
        # self.source_sql = Template(source_sql).render(self.jinja_data)
        # self.target_sql = Template(target_sql).render(self.jinja_data)

    def perpare_source(self):
        return self.spark.sql(self.source_sql).collect()[0].asDict()

    def perpare_target(self):
        return self.spark.sql(self.target_sql).collect()[0].asDict()

    def compare(self, source_result: dict, target_result: dict) -> bool:

        self.status = source_result == target_result

        if source_result.keys() != target_result.keys():
            print("keys not match: ", source_result.keys(), target_result.keys())

        diff = dict((k, target_result[k] - source_result[k])
                    for k in target_result)
        return {
            "status": self.status,
            "sources": self.sources,
            "targets": self.targets,
            "source_sql": self.source_sql,
            "target_sql": self.target_sql,
            "source_result": source_result,
            "target_result": target_result,
            "diff": diff,
            "jinja_data": self.jinja_data
        }


class ReconcilitionSuite():

    def __init__(
        self,
        spark,
        reconciler: ReconcilitionPair,
        suite: list,
        jinja_data: dict = {}
    ):
        self.suite = suite
        self.jinja_data = jinja_data
        self.reconciler = reconciler
        self.spark = spark

    def validate(self, output_format="json"):
        """
        Args:
            output_format (str, optional): json|dataframe. Defaults to "json". 

        Returns:
            _type_: json|dataframe
        """

        status_arr = []
        suite_rs = []
        run_id = str(uuid.uuid4())
        for item in self.suite:
            rs = self.reconciler(
                spark=self.spark,
                **item,
                jinja_data=self.jinja_data,
            ).validate()
            suite_rs.append({
                "run_id": run_id,
                "run_time": datetime.utcnow().isoformat(sep=' '),
                **rs})
            status_arr.append(rs['status'])
            
        success_cnt =  len([item for item in suite_rs if item['status']])
        suite_cnt = len(suite_rs)
        all_status = all(status_arr)
        summary_rs = {
            "run_id": run_id,
            "run_time": suite_rs[0]["run_time"],
            "status": all_status,
            "total_test": suite_cnt,
            "success": success_cnt,
            "success_percent": round(success_cnt/suite_cnt * 100, 2),
        }
        
        if output_format=="json":
            return suite_rs, summary_rs, all_status
        
        if output_format=="dataframe":
            schema = StructType([
                StructField('run_id', StringType(), True),
                StructField('run_time', StringType(), True),
                StructField('status', StringType(), True),
                StructField("sources", ArrayType(StringType()), True),
                StructField("targets", ArrayType(StringType()), True),
                StructField("source_sql", StringType(), True),
                StructField("target_sql", StringType(), True),
                StructField('source_result', StringType(), True),
                StructField('target_result', StringType(), True),
                StructField('diff', StringType(), True),
                StructField('jinja_data', StringType(), True),
            ])
            
            schema_summary = StructType([
                StructField('run_id', StringType(), True),
                StructField('run_time', StringType(), True),
                StructField('status', StringType(), True),
                StructField('total_test', LongType(), True),
                StructField('success', LongType(), True),
                StructField('success_percent', FloatType(), True),
            ])
            
            return self.spark.createDataFrame(
                data=suite_rs,
                schema=schema
            ), self.spark.createDataFrame(
                data=[summary_rs],
                schema=schema_summary
            ), all_status