"""``AbstractDataset`` implementation to access DeltaTables using
``delta-spark``.
"""
import warnings
from pathlib import PurePosixPath
from typing import Any, Dict, NoReturn

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets._io import AbstractDataset, DatasetError
from kedro_datasets.spark.spark_dataset import _split_filepath, _strip_dbfs_prefix


class DeltaTableDataset(AbstractDataset[None, DeltaTable]):
    """``DeltaTableDataset`` loads data into DeltaTable objects.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        weather@spark:
          type: spark.SparkDataset
          filepath: data/02_intermediate/data.parquet
          file_format: "delta"

        weather@delta:
          type: spark.DeltaTableDataset
          filepath: data/02_intermediate/data.parquet

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import StructField, StringType, IntegerType, StructType
        >>>
        >>> from kedro.extras.datasets.spark import DeltaTableDataset, SparkDataset
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>>
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> dataset = SparkDataset(filepath="test_data", file_format="delta")
        >>> dataset.save(spark_df)
        >>> deltatable_dataset = DeltaTableDataset(filepath="test_data")
        >>> delta_table = deltatable_dataset.load()
        >>>
        >>> delta_table.update()
    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead
    _SINGLE_PROCESS = True

    def __init__(self, filepath: str, metadata: Dict[str, Any] = None) -> None:
        """Creates a new instance of ``DeltaTableDataset``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                and working with data written to mount path points,
                specify ``filepath``s for (versioned) ``SparkDataset``s
                starting with ``/dbfs/mnt``.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        fs_prefix, filepath = _split_filepath(filepath)

        self._fs_prefix = fs_prefix
        self._filepath = PurePosixPath(filepath)
        self.metadata = metadata

    @staticmethod
    def _get_spark():
        return SparkSession.builder.getOrCreate()

    def _load(self) -> DeltaTable:
        load_path = self._fs_prefix + str(self._filepath)
        return DeltaTable.forPath(self._get_spark(), load_path)

    def _save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def _exists(self) -> bool:
        load_path = _strip_dbfs_prefix(self._fs_prefix + str(self._filepath))

        try:
            self._get_spark().read.load(path=load_path, format="delta")
        except AnalysisException as exception:
            # `AnalysisException.desc` is deprecated with pyspark >= 3.4
            message = exception.desc if hasattr(exception, "desc") else str(exception)
            if "Path does not exist:" in message or "is not a Delta table" in message:
                return False
            raise

        return True

    def _describe(self):
        return {"filepath": str(self._filepath), "fs_prefix": self._fs_prefix}


_DEPRECATED_CLASSES = {
    "DeltaTableDataSet": DeltaTableDataset,
}


def __getattr__(name):
    if name in _DEPRECATED_CLASSES:
        alias = _DEPRECATED_CLASSES[name]
        warnings.warn(
            f"{repr(name)} has been renamed to {repr(alias.__name__)}, "
            f"and the alias will be removed in Kedro-Datasets 2.0.0",
            KedroDeprecationWarning,
            stacklevel=2,
        )
        return alias
    raise AttributeError(f"module {repr(__name__)} has no attribute {repr(name)}")
