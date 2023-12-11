from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pyspark.sql import functions as F
import uuid
from jinja2 import Template


class DeequParser:
    """

    parse_json_check_suite
    parse_output_result_to_dataframe
    parse_html_output
    """

    @classmethod
    def parse_json_check_suite(cls, check: Check, check_suite: list) -> Check:
        """
        Parse Input:
        Args:
            check (pydeequ.checks.Check):
                check = Check(spark, CheckLevel.Warning, "test")
            check_suite (List[Any]):
                check_suite = [
                    {
                        "check": "hasSize",
                        "params": "lambda x: x >= 3"
                    },
                    {
                        "check": "hasMin",
                        "params": ["b", "lambda x: x == 0"]
                    },
                    {
                        "check": "isComplete",
                        "params": "c"
                    },
                    {
                        "check": "isUnique",
                        "params": ["a"]
                    },
                    {
                        "check": "isContainedIn",
                        "params": ["a", ["foo", "bar", "baz"]]
                    },
                    {
                        "check": "isNonNegative",
                        "params": "b"
                    }
                ]
        Examples:

        from pyspark.sql import SparkSession, Row
        spark = SparkSession.builder.appName('my_awesome').getOrCreate()
        from pydeequ.checks import *
        from pydeequ.verification import *

        df = spark.sparkContext.parallelize([
                    Row(a="foo", b=1, c=5),
                    Row(a="bar", b=2, c=6),
                    Row(a="baz", b=3, c=None)]).toDF()
        df.show()
        check_suite = [
            {
                "check": "hasSize",
                "params": "lambda x: x >= 3"
            },
            {
                "check": "hasMin",
                "params": ["b", "lambda x: x == 0"]
            },
            {
                "check": "isComplete",
                "params": "c"
            },
            {
                "check": "isUnique",
                "params": ["a"]
            },
            {
                "check": "isContainedIn",
                "params": ["a", ["foo", "bar", "baz"]]
            },
            {
                "check": "isNonNegative",
                "params": "b"
            }
        ]
        check = Check(spark, CheckLevel.Warning, "test")
        check = deequ_parse_json_check_suite(check, check_suite)
        checkResult = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(check).run()
        checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
        checkResult_df.show(10, False)
        """

        def _parse_params(params) -> list:
            if type(params) != list:
                params = [params]

            rs = []
            for param in params:
                _item = param
                if "lambda" in str(_item):
                    _item = eval(_item)
                rs.append(_item)

            return rs

        for _check in check_suite:
            getattr(check, _check['check'])(
                *_parse_params(_check['params']))

        return check

    @classmethod
    def parse_output_result_to_dataframe(
        cls,
        check_result,
        target_table: str,
        batch_id: str,
        spark,
    ):
        """

        Args:
            check_result (_type_): _description_
            target_table (str): _description_
            batch_id (str): _description_
            spark (_type_): _description_

        Returns:
            DataFrame

        """
        return (
            VerificationResult.checkResultsAsDataFrame(
                spark, check_result)
            .withColumn('run_dts', F.current_timestamp())
            .withColumn('run_dt', F.current_date())
            .withColumn('run_id', F.lit(batch_id))
            .withColumn('target_table', F.lit(target_table))
        )

    @classmethod
    def parse_team_html_output(cls, table, batch_id, result_details):
        html_template = """
        <link
        href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css"
        rel="stylesheet"
        />
        <!-- Google Fonts -->
        <link
        href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap"
        rel="stylesheet"
        />
        <!-- MDB -->
        <link
        href="https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/4.0.0/mdb.min.css"
        rel="stylesheet"
        />
        <script src="https://kit.fontawesome.com/a076d05399.js" crossorigin="anonymous"></script>
        <script
        type="text/javascript"
        src="https://cdnjs.cloudflare.com/ajax/libs/mdb-ui-kit/4.0.0/mdb.min.js"
        ></script>

        <table class="table align-middle mb-0 bg-white">
        <tbody>
            <tr style="background-color:#FB947D;">
            <td><p class="fw-normal mb-1">check_status</p></td>
            <td><p class="fw-normal mb-1">constraint</p></td>
            <td><p class="fw-normal mb-1">constraint_status</p></td>
            <td><p class="fw-normal mb-1">constraint_message</p></td>
            </tr>
            {% for item in result_details %}
            <tr>
            <td><p class="fw-small mb-1">{{ item.check_status }}</p></td>
            <td><p class="fw-normal mb-1">{{ item.constraint }}</p></td>
            <td><p class="fw-normal mb-1">{{ item.constraint_status }}</p></td>
            <td><p class="fw-normal mb-1">{{ item.constraint_message }}</p></td>
            {% endfor %}
        </tbody>
        </table>
        """

        jinja2_template = Template(html_template)
        html_stable = jinja2_template.render(
            result_details=result_details)

        sections = [
            {
                "activityTitle": "Quality Issue",
                "activityImage": "https://qmc.binus.ac.id/files/2017/12/quality-2470673_640.png",
                "facts": [
                    {"name": "table", "value": table},
                    {"name": "batch_id", "value": batch_id},
                ],
                "markdown": False
            },
            {
                "text": html_stable,
                "markdown": False
            }
        ]

        return {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "reconcile-job",
            "sections": sections,
        }


class DeequQuality:

    def __init__(
        self,
        spark,
        target_table: str,
        check_suite: list,
        check_level: str = "Warning",  # "Warning"|"Error"
        data_path: str = "s3://maxenv-t2-dev-bronze/meta/data_quality/",
        table_identifier: str = "iceberg_catalog.`maxenv-t2_db_meta`.data_quality"
    ):
        """
        {
            "target_table" : "test1",
            "check_level" : "Warning",
            "check_suite" : [
                {
                    "check": "hasSize",
                    "params": "lambda x: x >= 3"
                },
                {
                    "check": "hasMin",
                    "params": ["b", "lambda x: x == 0"]
                },
                {
                    "check": "isComplete",
                    "params": "c"
                },
                {
                    "check": "isUnique",
                    "params": ["a"]
                },
                {
                    "check": "isContainedIn",
                    "params": ["a", ["foo", "bar", "baz"]]
                },
                {
                    "check": "isNonNegative",
                    "params": "b"
                }
            ]
        }
        """
        self.spark = spark
        self.data_path = data_path
        self.table_identifier = table_identifier
        self.target_table = target_table
        self.check_level = check_level
        self.check_suite = check_suite

        self.check = DeequParser.parse_json_check_suite(
            Check(self.spark, CheckLevel[self.check_level], self.target_table),
            self.check_suite
        )

    def _parse_summary(self, _dq):
        df = (
            _dq
            .groupBy("target_table", "run_id", "run_dt", "run_dts")
            .pivot('constraint_status').agg(F.count("*"))
        )

        def _gen0col(colname):
            return F.col(colname) if colname in df.columns else F.lit(0)

        df = (
            df
            .withColumn("success",  _gen0col("Success"))
            .withColumn("failure",  _gen0col("Failure"))
            .withColumn("total", F.col("failure") + F.col("success"))
            .withColumn("success_percent", F.round(F.col("success") / F.col("total") * 100, 2))
        )

        return df.select(
            "target_table",
            "run_id",
            "run_dt",
            "run_dts",
            "total",
            "success",
            "success_percent"
        )

    def validate(self, df):

        run_id = str(uuid.uuid4())

        check_result = VerificationSuite(self.spark) \
            .onData(df) \
            .addCheck(self.check).run()

        result_df = DeequParser.parse_output_result_to_dataframe(
            check_result,
            self.target_table,
            run_id,
            self.spark
        )
        
        summary_rs = self._parse_summary(result_df)
        self.spark.sparkContext._gateway.shutdown_callback_server()
        # self.spark.stop()
        return result_df, summary_rs

# DeequQuality(
#     spark=spark,
#     target_table=kwargs["target_table"],
#     check_suite=kwargs["check_suite"],
#     check_level=kwargs.get("check_level"),
# ).validate(spark.sql(kwargs["sql"]))