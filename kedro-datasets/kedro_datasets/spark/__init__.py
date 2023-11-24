"""Provides I/O modules for Apache Spark."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
DeltaTableDataset: Any
SparkDataset: Any
SparkHiveDataset: Any
SparkJDBCDataset: Any
SparkStreamingDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "deltatable_dataset": ["DeltaTableDataset"],
        "spark_dataset": ["SparkDataset"],
        "spark_hive_dataset": ["SparkHiveDataset"],
        "spark_jdbc_dataset": ["SparkJDBCDataset"],
        "spark_streaming_dataset": ["SparkStreamingDataset"],
    },
)
