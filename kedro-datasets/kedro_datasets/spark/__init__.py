"""Provides I/O modules for Apache Spark."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
DeltaTableDataSet: Any
SparkDataSet: Any
SparkHiveDataSet: Any
SparkJDBCDataSet: Any
SparkStreamingDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "deltatable_dataset": ["DeltaTableDataSet"],
        "spark_dataset": ["SparkDataSet"],
        "spark_hive_dataset": ["SparkHiveDataSet"],
        "spark_jdbc_dataset": ["SparkJDBCDataSet"],
        "spark_streaming_dataset": ["SparkStreamingDataSet"],
    },
)
