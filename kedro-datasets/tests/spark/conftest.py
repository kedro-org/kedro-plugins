import os
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

        # 1. Disable PyArrow. PyArrow >= 19 often segfaults with PySpark 3.4 on Windows.
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
        builder = builder.config("spark.sql.execution.arrow.enabled", "false")

        # 2. Disable the Python Daemon. Forces a fresh python.exe for every task.
        #    This avoids memory/pickling corruption on Windows.
        builder = builder.config("spark.python.use.daemon", "false")
        builder = builder.config("spark.python.worker.reuse", "false")

        # 3. Pass HADOOP_HOME explicitly to the executor/worker processes
        #    Even if set in the shell, the spawned worker might miss it.
        builder = builder.config("spark.executorEnv.HADOOP_HOME", "C:\\hadoop")
        builder = builder.config("spark.executorEnv.hadoop.home.dir", "C:\\hadoop")

        # 4. Disable Native IO to prevent Hadoop DLL memory mapping crashes
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")

        # 5. Networking fixes for Windows CI
        builder = builder.config("spark.driver.host", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")

        # 6. Reduce partitions to speed up tests and reduce overhead
        builder = builder.config("spark.sql.shuffle.partitions", "1")

        if warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def spark_session(tmp_path_factory):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock = root_tmp_dir / "semaphore.lock"

    # Ensure warehouse exists
    warehouse_dir = root_tmp_dir / "spark-warehouse"
    if not warehouse_dir.exists():
        warehouse_dir.mkdir(parents=True, exist_ok=True)

    # Normalise path for Spark
    warehouse_path = warehouse_dir.as_posix()
    if sys.platform == "win32" and not warehouse_path.startswith("file:"):
        warehouse_path = f"file:///{warehouse_path}"

    with FileLock(lock):
        spark = _setup_spark_session(warehouse_path)
    yield spark
    spark.stop()
