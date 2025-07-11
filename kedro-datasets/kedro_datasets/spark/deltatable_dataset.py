"""``AbstractDataset`` implementation to access DeltaTables using
``delta-spark``.
"""
from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any, NoReturn

from delta.tables import DeltaTable
from kedro.io.core import AbstractDataset, DatasetError
from pyspark.sql.utils import AnalysisException

from kedro_datasets._utils.databricks_utils import split_filepath, strip_dbfs_prefix
from kedro_datasets._utils.spark_utils import get_spark


class DeltaTableDataset(AbstractDataset[None, DeltaTable]):
    """``DeltaTableDataset`` loads data into DeltaTable objects.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        weather@spark:
          type: spark.SparkDataset
          filepath: data/02_intermediate/data.parquet
          file_format: "delta"

        weather@delta:
          type: spark.DeltaTableDataset
          filepath: data/02_intermediate/data.parquet
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from delta import DeltaTable
        >>> from kedro_datasets.spark import DeltaTableDataset, SparkDataset
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import StructField, StringType, IntegerType, StructType
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> filepath = (tmp_path / "test_data").as_posix()
        >>> dataset = SparkDataset(filepath=filepath, file_format="delta")
        >>> dataset.save(spark_df)
        >>> deltatable_dataset = DeltaTableDataset(filepath=filepath)
        >>> delta_table = deltatable_dataset.load()
        >>> assert isinstance(delta_table, DeltaTable)

    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # using ``ThreadRunner`` instead
    _SINGLE_PROCESS = True

    def __init__(
        self, *, filepath: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Creates a new instance of ``DeltaTableDataset``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                and working with data written to mount path points,
                specify ``filepath``s for (versioned) ``SparkDataset``s
                starting with ``/dbfs/mnt``.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        fs_prefix, filepath = split_filepath(filepath)

        self._fs_prefix = fs_prefix
        self._filepath = PurePosixPath(filepath)
        self.metadata = metadata

    def load(self) -> DeltaTable:
        load_path = self._fs_prefix + str(self._filepath)
        return DeltaTable.forPath(get_spark(), load_path)

    def save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def _exists(self) -> bool:
        load_path = strip_dbfs_prefix(self._fs_prefix + str(self._filepath))

        try:
            get_spark().read.load(path=load_path, format="delta")
        except AnalysisException as exception:
            # `AnalysisException.desc` is deprecated with pyspark >= 3.4
            message = exception.desc if hasattr(exception, "desc") else str(exception)
            if "Path does not exist:" in message or "is not a Delta table" in message:
                return False
            raise

        return True

    def _describe(self):
        return {"filepath": str(self._filepath), "fs_prefix": self._fs_prefix}
