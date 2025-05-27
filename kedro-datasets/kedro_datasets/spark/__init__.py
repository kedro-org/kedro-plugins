"""Provides I/O modules for Apache Spark."""

from typing import Any

import lazy_loader as lazy

try:
    from .deltatable_dataset import DeltaTableDataset
except (ImportError, RuntimeError):
    DeltaTableDataset: Any

try:
    from .gbq_dataset import GBQQueryDataset
except (ImportError, RuntimeError):
    GBQQueryDataset: Any

try:
    from .spark_dataset import SparkDataset
except (ImportError, RuntimeError):
    SparkDataset: Any

try:
    from .spark_hive_dataset import SparkHiveDataset
except (ImportError, RuntimeError):
    SparkHiveDataset: Any

try:
    from .spark_jdbc_dataset import SparkJDBCDataset
except (ImportError, RuntimeError):
    SparkJDBCDataset: Any

try:
    from .spark_streaming_dataset import SparkStreamingDataset
except (ImportError, RuntimeError):
    SparkStreamingDataset: Any


__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "deltatable_dataset": ["DeltaTableDataset"],
        "gbq_dataset": ["GBQQueryDataset"],
        "spark_dataset": ["SparkDataset"],
        "spark_hive_dataset": ["SparkHiveDataset"],
        "spark_jdbc_dataset": ["SparkJDBCDataset"],
        "spark_streaming_dataset": ["SparkStreamingDataset"],
    },
)
