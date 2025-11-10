"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
import importlib.metadata
import os

import pandas as pd
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

DELTA_VERSION = importlib.metadata.version("delta-spark")


@pytest.fixture(scope="class", autouse=True)
def spark_session():

    # Delta 4.0+ uses different Maven coordinates
    major_version = int(DELTA_VERSION.split('.')[0])

    if major_version >= 4:
        # Delta 4.x with PySpark 4.x uses Scala 2.13
        delta_package = f"io.delta:delta-spark_2.13:{DELTA_VERSION}"
    else:
        # Delta 2.x/3.x with PySpark 3.x uses Scala 2.12
        delta_package = f"io.delta:delta-core_2.12:{DELTA_VERSION}"

    spark = (
        SparkSession.builder.appName("test")
        .config("spark.jars.packages", delta_package)
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
