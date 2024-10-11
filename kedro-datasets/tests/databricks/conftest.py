"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
import os

# importlib_metadata needs backport for python 3.8 and older
import importlib_metadata
import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

DELTA_VERSION = importlib_metadata.version("delta-spark")


@pytest.fixture(scope="class", autouse=True)
def spark_session():
    spark = (
        SparkSession.builder.appName("test")
        .config("spark.jars.packages", f"io.delta:delta-core_2.12:{DELTA_VERSION}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sql("create database if not exists test")
    yield spark
    spark.sql("drop database test cascade;")


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


@pytest.fixture
def external_location():
    return os.environ.get("DATABRICKS_EXTERNAL_LOCATION")
