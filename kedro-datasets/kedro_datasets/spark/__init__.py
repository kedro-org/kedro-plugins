"""Provides I/O modules for Apache Spark."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
DeltaTableDataSet: type[DeltaTableDataset]
DeltaTableDataset: Any
SparkDataSet: type[SparkDataset]
SparkDataset: Any
SparkHiveDataSet: type[SparkHiveDataset]
SparkHiveDataset: Any
SparkJDBCDataSet: type[SparkJDBCDataset]
SparkJDBCDataset: Any
SparkStreamingDataSet: type[SparkStreamingDataset]
SparkStreamingDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "deltatable_dataset": ["DeltaTableDataSet", "DeltaTableDataset"],
        "spark_dataset": ["SparkDataSet", "SparkDataset"],
        "spark_hive_dataset": ["SparkHiveDataSet", "SparkHiveDataset"],
        "spark_jdbc_dataset": ["SparkJDBCDataSet", "SparkJDBCDataset"],
        "spark_streaming_dataset": ["SparkStreamingDataSet", "SparkStreamingDataset"],
    },
)
