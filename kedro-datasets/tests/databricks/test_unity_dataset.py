import pytest
from kedro.io.core import DataSetError, VersionNotFoundError
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql import DataFrame, SparkSession
import pandas as pd
from kedro_datasets.databricks import UnityTableDataSet


@pytest.fixture
def sample_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def upsert_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("Alex", 32), ("Evan", 23)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def mismatched_upsert_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ]
    )

    data = [("Alex", 32, 174), ("Evan", 23, 166)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def subset_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", IntegerType(), True),
        ]
    )

    data = [("Alex", 32, 174), ("Evan", 23, 166)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def subset_pandas_df():
    return pd.DataFrame(
        {"name": ["Alex", "Evan"], "age": [32, 23], "height": [174, 166]}
    )


@pytest.fixture
def subset_expected_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("Alex", 32), ("Evan", 23)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_pandas_df():
    return pd.DataFrame(
        {"name": ["Alex", "Bob", "Clarke", "Dave"], "age": [31, 12, 65, 29]}
    )


@pytest.fixture
def append_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("Evan", 23), ("Frank", 13)]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_append_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [
        ("Alex", 31),
        ("Bob", 12),
        ("Clarke", 65),
        ("Dave", 29),
        ("Evan", 23),
        ("Frank", 13),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_upsert_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [
        ("Alex", 32),
        ("Bob", 12),
        ("Clarke", 65),
        ("Dave", 29),
        ("Evan", 23),
    ]

    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_upsert_multiple_primary_spark_df(spark_session: SparkSession):
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [
        ("Alex", 31),
        ("Alex", 32),
        ("Bob", 12),
        ("Clarke", 65),
        ("Dave", 29),
        ("Evan", 23),
    ]

    return spark_session.createDataFrame(data, schema)


class TestUnityTableDataSet:
    def test_full_table(self):
        unity_ds = UnityTableDataSet(catalog="test", database="test", table="test")
        assert unity_ds._full_table_address == "test.test.test"

    def test_database_table(self):
        unity_ds = UnityTableDataSet(database="test", table="test")
        assert unity_ds._full_table_address == "test.test"

    def test_table_only(self):
        unity_ds = UnityTableDataSet(table="test")
        assert unity_ds._full_table_address == "default.test"

    def test_table_missing(self):
        with pytest.raises(TypeError):
            UnityTableDataSet()

    def test_describe(self):
        unity_ds = UnityTableDataSet(table="test")
        assert unity_ds._describe() == {
            "catalog": None,
            "database": "default",
            "table": "test",
            "write_mode": "overwrite",
            "dataframe_type": "spark",
            "primary_key": None,
            "version": None,
        }

    def test_invalid_write_mode(self):
        with pytest.raises(DataSetError):
            UnityTableDataSet(table="test", write_mode="invalid")

    def test_dataframe_type(self):
        with pytest.raises(DataSetError):
            UnityTableDataSet(table="test", dataframe_type="invalid")

    def test_missing_primary_key_upsert(self):
        with pytest.raises(DataSetError):
            UnityTableDataSet(table="test", write_mode="upsert")

    def test_schema(self):
        unity_ds = UnityTableDataSet(
            table="test",
            schema={
                "fields": [
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "age",
                        "nullable": True,
                        "type": "integer",
                    },
                ],
                "type": "struct",
            },
        )
        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        assert unity_ds._schema == expected_schema

    def test_catalog_exists(self):
        unity_ds = UnityTableDataSet(catalog="test", database="invalid", table="test_not_there")
        assert not unity_ds._exists()

    def test_table_does_not_exist(self):
        unity_ds = UnityTableDataSet(database="invalid", table="test_not_there")
        assert not unity_ds._exists()

    def test_save_default(self, sample_spark_df: DataFrame):
        unity_ds = UnityTableDataSet(database="test", table="test_save")
        unity_ds.save(sample_spark_df)
        saved_table = unity_ds.load()
        assert unity_ds.exists() and sample_spark_df.exceptAll(saved_table).count() == 0

    def test_save_schema_spark(
        self, subset_spark_df: DataFrame, subset_expected_df: DataFrame
    ):
        unity_ds = UnityTableDataSet(
            database="test",
            table="test_save_spark_schema",
            schema={
                "fields": [
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "age",
                        "nullable": True,
                        "type": "integer",
                    },
                ],
                "type": "struct",
            },
        )
        unity_ds.save(subset_spark_df)
        saved_table = unity_ds.load()
        assert subset_expected_df.exceptAll(saved_table).count() == 0

    def test_save_schema_pandas(
        self, subset_pandas_df: pd.DataFrame, subset_expected_df: DataFrame
    ):
        unity_ds = UnityTableDataSet(
            database="test",
            table="test_save_pd_schema",
            schema={
                "fields": [
                    {
                        "metadata": {},
                        "name": "name",
                        "nullable": True,
                        "type": "string",
                    },
                    {
                        "metadata": {},
                        "name": "age",
                        "nullable": True,
                        "type": "integer",
                    },
                ],
                "type": "struct",
            },
            dataframe_type="pandas",
        )
        unity_ds.save(subset_pandas_df)
        saved_ds = UnityTableDataSet(
            database="test",
            table="test_save_pd_schema",
        )
        saved_table = saved_ds.load()
        assert subset_expected_df.exceptAll(saved_table).count() == 0

    def test_save_overwrite(
        self, sample_spark_df: DataFrame, append_spark_df: DataFrame
    ):
        unity_ds = UnityTableDataSet(database="test", table="test_save")
        unity_ds.save(sample_spark_df)
        unity_ds.save(append_spark_df)

        overwritten_table = unity_ds.load()

        assert append_spark_df.exceptAll(overwritten_table).count() == 0

    def test_save_append(
        self,
        sample_spark_df: DataFrame,
        append_spark_df: DataFrame,
        expected_append_spark_df: DataFrame,
    ):
        unity_ds = UnityTableDataSet(
            database="test", table="test_save_append", write_mode="append"
        )
        unity_ds.save(sample_spark_df)
        unity_ds.save(append_spark_df)

        appended_table = unity_ds.load()

        assert expected_append_spark_df.exceptAll(appended_table).count() == 0

    def test_save_upsert(
        self,
        sample_spark_df: DataFrame,
        upsert_spark_df: DataFrame,
        expected_upsert_spark_df: DataFrame,
    ):
        unity_ds = UnityTableDataSet(
            database="test",
            table="test_save_upsert",
            write_mode="upsert",
            primary_key="name",
        )
        unity_ds.save(sample_spark_df)
        unity_ds.save(upsert_spark_df)

        upserted_table = unity_ds.load()

        assert expected_upsert_spark_df.exceptAll(upserted_table).count() == 0

    def test_save_upsert_multiple_primary(
        self,
        sample_spark_df: DataFrame,
        upsert_spark_df: DataFrame,
        expected_upsert_multiple_primary_spark_df: DataFrame,
    ):
        unity_ds = UnityTableDataSet(
            database="test",
            table="test_save_upsert_multiple",
            write_mode="upsert",
            primary_key=["name", "age"],
        )
        unity_ds.save(sample_spark_df)
        unity_ds.save(upsert_spark_df)

        upserted_table = unity_ds.load()

        assert (
            expected_upsert_multiple_primary_spark_df.exceptAll(upserted_table).count()
            == 0
        )

    def test_save_upsert_mismatched_columns(
        self,
        sample_spark_df: DataFrame,
        mismatched_upsert_spark_df: DataFrame,
    ):
        unity_ds = UnityTableDataSet(
            database="test",
            table="test_save_upsert_mismatch",
            write_mode="upsert",
            primary_key="name",
        )
        unity_ds.save(sample_spark_df)
        with pytest.raises(DataSetError):
            unity_ds.save(mismatched_upsert_spark_df)

    def test_load_spark(self, sample_spark_df: DataFrame):
        unity_ds = UnityTableDataSet(database="test", table="test_load_spark")
        unity_ds.save(sample_spark_df)

        delta_ds = UnityTableDataSet(database="test", table="test_load_spark")
        delta_table = delta_ds.load()

        assert (
            isinstance(delta_table, DataFrame)
            and delta_table.exceptAll(sample_spark_df).count() == 0
        )

    def test_load_spark_no_version(self, sample_spark_df: DataFrame):
        unity_ds = UnityTableDataSet(database="test", table="test_load_spark")
        unity_ds.save(sample_spark_df)

        delta_ds = UnityTableDataSet(
            database="test", table="test_load_spark", version=2
        )
        with pytest.raises(VersionNotFoundError):
            _ = delta_ds.load()

    def test_load_version(self, sample_spark_df: DataFrame, append_spark_df: DataFrame):
        unity_ds = UnityTableDataSet(
            database="test", table="test_load_version", write_mode="append"
        )
        unity_ds.save(sample_spark_df)
        unity_ds.save(append_spark_df)

        loaded_ds = UnityTableDataSet(
            database="test", table="test_load_version", version=0
        )
        loaded_df = loaded_ds.load()

        assert loaded_df.exceptAll(sample_spark_df).count() == 0

    def test_load_pandas(self, sample_pandas_df: pd.DataFrame):
        unity_ds = UnityTableDataSet(
            database="test", table="test_load_pandas", dataframe_type="pandas"
        )
        unity_ds.save(sample_pandas_df)

        pandas_ds = UnityTableDataSet(
            database="test", table="test_load_pandas", dataframe_type="pandas"
        )
        pandas_df = pandas_ds.load().sort_values("name", ignore_index=True)

        assert isinstance(pandas_df, pd.DataFrame) and pandas_df.equals(
            sample_pandas_df
        )
