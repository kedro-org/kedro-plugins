import pytest
from kedro_datasets.databricks import ManagedTableDataset

# importlib_metadata needs backport for python 3.8 and older
import importlib_metadata
from pyspark.sql import SparkSession

DELTA_VERSION = importlib_metadata.version("delta-spark")

@pytest.fixture()
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
    spark.catalog.clearCache()
    spark.sql("drop database test cascade;")

class TestManagedTableDataset:
    def test_describe(self, spark_session):
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
