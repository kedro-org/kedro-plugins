"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""

import shutil
import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from filelock import FileLock
from pyspark.sql import SparkSession


def _setup_spark_session(warehouse_dir=None):
    builder = (
        SparkSession.builder.appName("KedroSparkTest")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    if sys.platform == "win32":
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")

        # Fix localhost resolution slowness
        builder = builder.config("spark.driver.host", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")

        # Explicit warehouse dir to ensure we stay inside C:\tmp
        if warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def spark_session(tmp_path_factory):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock = root_tmp_dir / "semaphore.lock"

    warehouse_dir = root_tmp_dir / "spark-warehouse"

    # Ensure the warehouse exists
    if not warehouse_dir.exists():
        warehouse_dir.mkdir(parents=True, exist_ok=True)

    # Convert to posix path (forward slashes) for Spark URL compatibility
    warehouse_posix = warehouse_dir.as_posix()
    if sys.platform == "win32" and not warehouse_posix.startswith("file:"):
        warehouse_posix = f"file:///{warehouse_posix}"

    with FileLock(lock):
        spark = _setup_spark_session(warehouse_posix)

    yield spark
    spark.stop()
