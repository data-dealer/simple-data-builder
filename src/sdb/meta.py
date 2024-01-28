from yaml import load, loader


class TableMeta:
    """
    TableMeta(from_txt)
    ```yaml
    version: 1.0.0

    model:
        table_name: sat_ltindang
        database_name: maxenv-t2_db_silver
        data_location: s3://maxenv-t2-dev-silver/iceberg/sat_ltindang/data/
        checkpoint: s3://maxenv-t2-dev-silver/iceberg/sat_ltindang/checkpoint
        partition_by: null
        data_format: iceberg
        columns:
            - name: dv_hashkey_listing
            type: string
            tests:
                - check: isComplete
                - check: isUnique
            description: dv hashkey

            - name: dv_recsource
            type: string
            tests:
                - check: isComplete
            description: dv columns

            - name: dt
            type: int
            tests:
                - check: isComplete
                - check: hasMin
                    params: "lambda x: x == 60"

            - name: gia
            type: int
            tests:
                - check: isComplete
                - check: hasMin
                    params: "lambda x: x == 2000"

            - name: latitude
            type: float
            tests:
                - check: isComplete

            - name: longitude
            type: float
            tests:
                - check: isComplete

            - name: meta
            type: map<string,string>

        depends:
            - table_name: ltindang
            database_name: maxenv-t2_db_bronze

        sla:
            - streaming

        reconciles:
            - sources:
                - bronze.ltindang
                targets:
                - silver.sat_ltindang
                source_sql: select COUNT(DISTINCT matin) as cnt_distinct from iceberg_catalog.`maxenv-t2_db_bronze`.ltindang
                target_sql: select count(*) as cnt_distinct from iceberg_catalog.`maxenv-t2_db_silver`.sat_ltindang
    ```
    """

    def __init__(self, from_text=None, from_key=None):
        if from_text:
            self.meta = load(from_text, Loader=loader.SafeLoader)
        else:
            import boto3
            s3_client = boto3.client('s3')
            response = s3_client.get_object(
                Bucket="maxenv-t2-dev-artifacts",
                Key=from_key
            )
            self.meta = load(response["Body"], Loader=loader.SafeLoader)

        self.model = self.meta['model']
        self.reconciles = self.meta['reconciles']
        self.depends = self.meta['depends']
        self.sla = self.meta['sla']

    @property
    def table_identifier(self):
        table_identifier = f"iceberg_catalog.`{self.model['database_name']}`.{self.model['table_name']}"
        if self.model.get('catalog'):
            table_identifier = f"{self.model.get('catalog')}.`{self.model['database_name']}`.{self.model['table_name']}"
        return table_identifier

    @property
    def struct_type(self):
        from pyspark.sql.types import StructType
        fields = []
        for item in self.model['columns']:
            if not item.get("nullable"):
                item["nullable"] = True
            if not item.get("metadata"):
                item["metadata"] = None
            fields.append(item)

        return StructType.fromJson({"fields": fields})

    @property
    def ddl_sql(self):
        partition_by = self.model.get('partition_by')
        if type(partition_by) == list:
            partition_by = ",".join(partition_by) if len(
                partition_by) > 0 else None
        partition_sql = f"PARTITIONED BY ({partition_by})" if partition_by else ""

        fields = ",\n".join(
            [f"{col['name']} {col['type']}" for col in self.model['columns']])

        return f"""CREATE TABLE IF NOT EXISTS {self.table_identifier}
(
{fields}
)
USING {self.model['data_format']}
LOCATION '{self.model['data_location']}'
{partition_sql}
TBLPROPERTIES (
    'table_type'='iceberg',
    'format'='parquet'
)"""

    @property
    def deequ_quality_params(self):
        check_suite = []

        for item in self.model['columns']:
            name = item['name']
            tests = item.get("tests")
            if tests:
                for _test in tests:
                    check_suite.append({
                        "check": _test["check"],
                        "params": [name, *_test.get("params")] if _test.get("params") else [name]
                    })

        return check_suite

    @property
    def ge_quality_params(self):
        check_suite = []

        for item in self.model['columns']:
            name = item['name']
            tests = item.get("tests")
            if tests:
                for _test in tests:
                    if "expect" in _test["check"]:
                        check_suite.append({
                            "check": _test["check"],
                            "kwargs": {"columns": name, **_test.get("kwargs")} if _test.get("kwargs") else  {"columns": name}
                        })

        return check_suite

    def execute_dll(self, spark):
        spark.sql(self.ddl_sql)


# table_utils = TableMeta(from_key="glue_metas/silver/link_account_listing_district/meta.yml")
# target_table = table_utils.model['table_name']
# suite = table_utils.reconciles