import pytest
from kedro_datasets.databricks import ManagedTableDataset

# importlib_metadata needs backport for python 3.8 and older
import importlib_metadata
from pyspark.sql import SparkSession

DELTA_VERSION = importlib_metadata.version("delta-spark")


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
