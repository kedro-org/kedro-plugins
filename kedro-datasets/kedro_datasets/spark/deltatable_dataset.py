"""``AbstractDataSet`` implementation to access DeltaTables using
``delta-spark``
"""
from pathlib import PurePosixPath
from typing import Any, Dict, NoReturn

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from kedro_datasets.spark.spark_dataset import _split_filepath, _strip_dbfs_prefix

from .._io import AbstractDataset as AbstractDataSet
from .._io import DatasetError as DataSetError


class DeltaTableDataSet(AbstractDataSet[None, DeltaTable]):
    """``DeltaTableDataSet`` loads data into DeltaTable objects.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html#data-catalog-yaml-examples>`_:

    .. code-block:: yaml

        weather@spark:
          type: spark.SparkDataSet
          filepath: data/02_intermediate/data.parquet
          file_format: "delta"

        weather@delta:
          type: spark.DeltaTableDataSet
          filepath: data/02_intermediate/data.parquet

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html#data-catalog-yaml-examples>`_:
    ::

        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import (StructField, StringType,
        >>>                                IntegerType, StructType)
        >>>
        >>> from kedro.extras.datasets.spark import DeltaTableDataSet, SparkDataSet
        >>>
        >>> schema = StructType([StructField("name", StringType(), True),
        >>>                      StructField("age", IntegerType(), True)])
        >>>
        >>> data = [('Alex', 31), ('Bob', 12), ('Clarke', 65), ('Dave', 29)]
        >>>
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> data_set = SparkDataSet(filepath="test_data", file_format="delta")
        >>> data_set.save(spark_df)
        >>> deltatable_dataset = DeltaTableDataSet(filepath="test_data")
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
        """Creates a new instance of ``DeltaTableDataSet``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                and working with data written to mount path points,
                specify ``filepath``s for (versioned) ``SparkDataSet``s
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
        raise DataSetError(f"{self.__class__.__name__} is a read only dataset type")

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
