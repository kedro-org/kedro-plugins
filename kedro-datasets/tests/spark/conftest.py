import pytest
import sys
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from filelock import FileLock
from pyspark.sql import SparkSession


def _setup_spark_session(warehouse_dir=None):
    builder = SparkSession.builder.appName("KedroSparkTest") \
        .master("local[1]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    if sys.platform == "win32":
        # --- CRITICAL WINDOWS FIXES ---
        # 1. Disable the Python Daemon. This prevents the "Worker Crashed" error
        #    common with Python 3.11+ on Windows.
        builder = builder.config("spark.python.use.daemon", "false")
        builder = builder.config("spark.python.worker.reuse", "false")
        
        # 2. Disable Native IO to prevent Hadoop DLL memory mapping crashes
        builder = builder.config("spark.hadoop.io.native.lib.available", "false")

        # 3. Disable Arrow optimization (often unstable on Windows CI)
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
        
        # 4. Fix Localhost lookup slowness
        builder = builder.config("spark.driver.host", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1")

        # 5. Set warehouse dir to the safe short path
        if warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)

    return configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="module", autouse=True)
def spark_session(tmp_path_factory):
    root_tmp_dir = tmp_path_factory.getbasetemp().parent
    lock = root_tmp_dir / "semaphore.lock"
    # Windows: Use the short path setup from Makefile/YAML
    warehouse_dir = (root_tmp_dir / "spark-warehouse")
    if not warehouse_dir.exists():
        warehouse_dir.mkdir(parents=True, exist_ok=True)

    # Ensure POSIX path format for Spark (file:///)
    warehouse_path_str = warehouse_dir.as_posix()
    if sys.platform == "win32" and not warehouse_path_str.startswith("file:"):
        warehouse_path_str = f"file:///{warehouse_path_str}"

    with FileLock(lock):
        spark = _setup_spark_session(warehouse_path_str)
    yield spark
    spark.stop()
