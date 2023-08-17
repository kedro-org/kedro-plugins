"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""
import importlib.metadata

import pytest
from pyspark.sql import SparkSession

DELTA_VERSION  = importlib.metadata.version("delta-spark")


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
