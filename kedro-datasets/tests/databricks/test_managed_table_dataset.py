import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kedro_datasets.databricks import ManagedTableDataset


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


class TestManagedTableDataset:
    def test_describe(self):
        unity_ds = ManagedTableDataset(table="test")
        assert unity_ds._describe() == {
            "catalog": None,
            "database": "default",
            "table": "test",
            "write_mode": None,
            "dataframe_type": "spark",
            "primary_key": None,
            "version": "None",
            "owner_group": None,
            "partition_columns": None,
        }
