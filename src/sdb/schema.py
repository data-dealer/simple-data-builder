from __future__ import annotations

from abc import ABC, abstractmethod

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame


class EvolutionPattern(ABC):
    def __init__(self, df_origin, df_stage):
        self.df_origin = df_origin
        self.df_stage = df_stage

    @abstractmethod
    def identifi_conflict(self) -> dict:
        pass
        # return conflict

    @abstractmethod
    def resolve_conflict(self) -> (DataFrame, dict, list):
        """apply for incomming (stage) dataframe
        return resolved dataframe, conflict dict, cannot resolve list
        """
        pass


class EvolutionMissingColumn(EvolutionPattern):
    """
    missing_column_in_origin
    missing_column_in_stage
    """

    def identifi_conflict(self) -> dict:
        """
        Returns: {
            'EVOLUTIONMISSINGCOLUMN_ORIGIN': {
                'content': '(age: bigint)',
                'fields': [StructField('age', LongType(), True)]
            },
            'EVOLUTIONMISSINGCOLUMN_STAGE': {
                'content': '(gender: string), (code: string), (tuoi: string)',
                'fields': [StructField('gender', StringType(), True),
                        StructField('code', StringType(), True),
                        StructField('tuoi', StringType(), True)]
            }
        }
        """
        conflict = {}

        def _gen_missing(df_origin, df_stage):
            _cols1 = {field.name for field in df_origin.schema.fields}
            _cols2 = {field.name for field in df_stage.schema.fields}

            origin_missing = list(_cols2 - _cols1)
            origin_missing_schema = ", ".join(
                [
                    f"({field.name}: {field.dataType.simpleString()})"
                    for field in df_stage.select(*origin_missing).schema.fields
                ]
            )

            return (
                origin_missing_schema,
                df_stage.select(*origin_missing).schema.fields,
            )

        origin_missing_schema, origin_missing_field = _gen_missing(
            self.df_origin, self.df_stage
        )
        stage_missing_schema, stage_missing_field = _gen_missing(
            self.df_stage, self.df_origin
        )
        if len(origin_missing_field) > 0:
            conflict[self.__class__.__name__ + "Origin"] = {
                "content": origin_missing_schema,
                "fields": origin_missing_field,
            }
        if len(stage_missing_field) > 0:
            conflict[self.__class__.__name__ + "Stage"] = {
                "content": stage_missing_schema,
                "fields": stage_missing_field,
            }

        return conflict

    def resolve_conflict(self):
        """
        1. lit null missing columns
        2. select:
            - origin schema (include missing)
            - redundant columns  of stage df
        """
        conflict = self.identifi_conflict()

        fields = conflict[self.__class__.__name__ + "Stage"]["fields"]
        for field in fields:
            self.df_stage = self.df_stage.withColumn(
                field.name, F.lit(None).cast(field.dataType)
            )

        return self.df_stage, conflict, []


class EvolutionConflictType(EvolutionPattern):
    """
    handle 1 level struct: map<string:string>, array<string>
    for more complex case => advance flatten in downstream
    """

    def identifi_conflict(self) -> dict:
        """
        Returns: {
            'EvolutionConflictType': [
                {
                    'column': 'id',
                    'original': 'bigint',
                    'stage': 'string'
                },
                {
                    'column': 'count',
                    'original': 'bigint',
                    'stage': 'string'
                }
            ]
        }
        """
        conflict = {}
        _cols1 = [field.name for field in self.df_origin.schema.fields]
        _cols2 = [field.name for field in self.df_stage.schema.fields]

        common_cols = set(_cols1).intersection(_cols2)
        # compare common columns

        _common_pairs = []
        for field1, field2 in zip(
            self.df_origin.select(*common_cols).schema.fields,
            self.df_stage.select(*common_cols).schema.fields,
        ):
            if str(field1.dataType) != str(field2.dataType):
                _common_pairs.append(
                    {
                        "column": field1.name,
                        "original": field1.dataType.simpleString(),
                        "stage": field2.dataType.simpleString(),
                    }
                )

        if len(_common_pairs) > 0:
            conflict[self.__class__.__name__] = _common_pairs

        return conflict

    def resolve_conflict(self):
        conflict = self.identifi_conflict()
        cannot_solved = []
        for field in conflict[self.__class__.__name__]:
            _col, _origin, _stg = (field["column"], field["original"], field["stage"])
            try:
                # handle convert string to map
                if _origin.startswith("map") and _stg == "string":
                    self.df_stage = self.df_stage.withColumn(
                        _col, F.from_json(_col, _origin)
                    )
                # handle convert string to array,
                elif _origin.startswith("array") and _stg == "string":
                    self.df_stage = self.df_stage.withColumn(
                        _col, F.from_json(_col, _origin)
                    )
                else:
                    self.df_stage = self.df_stage.withColumn(
                        _col, F.col(_col).cast(_origin)
                    )
            except Exception as e:
                cannot_solved.append({**field, "error": e})
        return self.df_stage, conflict, cannot_solved


def evolution_handle(df_origin, df_stage):
    engines = [EvolutionMissingColumn, EvolutionConflictType]
    conflict = {}
    cannot_solved = []
    for engine in engines:
        df_stage, _conflict, _cannot_solved = engine(
            df_origin, df_stage
        ).resolve_conflict()
        conflict.update(_conflict)
        cannot_solved += _cannot_solved

    return df_stage, conflict, cannot_solved
