"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""

import sys

import pytest
from delta import configure_spark_with_delta_pip
from filelock import FileLock
from pyspark.sql import SparkSession


def _setup_spark_session(warehouse_dir=None):
    builder = (
        SparkSession.builder.appName("MyApp")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    # --- Windows Specific Configuration ---
    if sys.platform == "win32" and warehouse_dir:
        # 1. Use the short path passed from the fixture to avoid path limit errors
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
        # 2. Fix for "LocalHost lookup" slowness on Windows CI
        builder = builder.config("spark.driver.host", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")
        # 3. Fix for specific Windows/Hadoop native library crashes
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def spark_session(tmp_path_factory):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock = root_tmp_dir / "semaphore.lock"

    # We only care about explicitly setting this on Windows
    warehouse_dir = None
    if sys.platform == "win32":
        warehouse_dir = (root_tmp_dir / "spark-warehouse").as_posix()

    with FileLock(lock):
        spark = _setup_spark_session(warehouse_dir)

    yield spark
    spark.stop()
