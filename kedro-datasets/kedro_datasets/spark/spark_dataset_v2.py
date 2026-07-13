"""``AbstractVersionedDataset`` implementation to access Spark dataframes using
``pyspark``.
"""

from __future__ import annotations

import logging
import os
from copy import deepcopy
from functools import partial
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
)

if TYPE_CHECKING:
    import pandas as pd
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

from kedro_datasets._utils.databricks_utils import (
    dbfs_exists,
    dbfs_glob,
    deployed_on_databricks,
    get_dbutils,
    parse_spark_filepath,
    to_spark_path,
    validate_databricks_path,
)
from kedro_datasets._utils.spark_utils import (
    get_spark_filesystem,
    get_spark_with_remote_support,
    load_spark_schema_from_file,
)

logger = logging.getLogger(__name__)


class SparkDatasetV2(AbstractVersionedDataset):
    """``SparkDatasetV2`` loads and saves Spark dataframes.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        weather:
          type: spark.SparkDatasetV2
          filepath: s3a://your_bucket/data/01_raw/weather/*
          file_format: csv
          load_args:
            header: True
            inferSchema: True
          save_args:
            sep: '|'
            header: True

        weather_with_schema:
          type: spark.SparkDatasetV2
          filepath: s3a://your_bucket/data/01_raw/weather/*
          file_format: csv
          load_args:
            header: True
            schema:
              filepath: path/to/schema.json
          save_args:
            sep: '|'
            header: True

        weather_cleaned:
          type: spark.SparkDatasetV2
          filepath: data/02_intermediate/data.parquet
          file_format: parquet

        # Databricks with Unity Catalog
        unity_data:
          type: spark.SparkDatasetV2
          filepath: /Volumes/catalog/schema/volume/data.parquet

        # Databricks with DBFS
        dbfs_data:
          type: spark.SparkDatasetV2
          filepath: /dbfs/mnt/data/output.parquet
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> import tempfile
        >>> from pyspark.sql import Row, SparkSession
        >>> from pyspark.sql.types import IntegerType, StringType, StructField, StructType
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> with tempfile.TemporaryDirectory() as tmp_dir:
        ...     filepath = f"{tmp_dir}/test_data"
        ...     dataset = SparkDatasetV2(filepath=filepath)
        ...     dataset.save(spark_df)
        ...     reloaded = dataset.load()
        ...     assert Row(name="Bob", age=12) in reloaded.take(4)

        You can also save Pandas DataFrames directly they will be automatically
        converted to Spark DataFrames:

        >>> import pandas as pd
        >>> pandas_df = pd.DataFrame({"name": ["Alex", "Bob"], "age": [31, 12]})
        >>> dataset.save(pandas_df)  # Automatically converts to Spark DataFrame
    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # ``ThreadRunner`` instead
    _SINGLE_PROCESS = True
    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        file_format: str = "parquet",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``SparkDatasetV2``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. Supports:
                - Local paths: ``data/output.parquet`` or ``/absolute/path/data.parquet``
                - S3: ``s3://bucket/path`` or ``s3a://bucket/path``
                - GCS: ``gs://bucket/path``
                - Azure: ``abfs://container@account.dfs.core.windows.net/path``
                - Databricks DBFS: ``/dbfs/path`` or ``dbfs:/path``
                - Unity Catalog: ``/Volumes/catalog/schema/volume/path``
            file_format: File format used during load and save
                operations. These are formats supported by the running
                SparkContext include parquet, csv, delta. For a list of supported
                formats please refer to Apache Spark documentation at
                https://spark.apache.org/docs/latest/sql-programming-guide.html
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
            save_args: Save args passed to Spark DataFrame write options.
                Similar to load_args this is dependent on the selected file
                format. You can pass ``mode`` and ``partitionBy`` to specify
                your overwrite mode and partitioning respectively. You can find
                a list of options for each format in Spark DataFrame
                write documentation:
                https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials to access cloud storage, such as
                ``key``, ``secret`` for S3, ``token`` for GCS, or
                ``account_key`` for Azure. Structure depends on the
                cloud provider.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        # Store original filepath for reference
        self._original_filepath = filepath

        # Process credentials
        credentials = deepcopy(credentials) or {}

        # Parse filepath and detect protocol
        protocol, path = parse_spark_filepath(filepath)

        # Handle relative paths for local files
        if protocol in ("file", "") and not os.path.isabs(path):
            path = str(Path(path).resolve())
            protocol = "file"  # Normalise empty to "file"

        self._protocol = protocol
        self._path = path

        # Validate Databricks paths
        validate_databricks_path(filepath)

        # Get filesystem operations
        exists_function, glob_function = self._get_filesystem_ops(
            protocol, filepath, credentials
        )

        # Initialise attributes
        self._file_format = file_format
        self._load_args = {
            **self.DEFAULT_LOAD_ARGS,
            **(deepcopy(load_args) if load_args is not None else {}),
        }
        self._save_args = {
            **self.DEFAULT_SAVE_ARGS,
            **(deepcopy(save_args) if save_args is not None else {}),
        }
        self._credentials = credentials
        self.metadata = metadata

        # Handle schema - can be a dict with filepath or a StructType directly
        self._schema: StructType | None = self._load_args.pop("schema", None)
        if self._schema is not None and isinstance(self._schema, dict):
            self._schema = load_spark_schema_from_file(self._schema)

        # Call parent constructor
        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=exists_function,
            glob_function=glob_function,
        )

        # Validate delta format
        self._handle_delta_format()

    @property
    def _spark_path(self) -> str:
        """Get the Spark-compatible path for this dataset.

        Returns:
            Path formatted for Spark (e.g., 's3a://bucket/path', 'file:///path').
        """
        return to_spark_path(self._protocol, self._path)

    def _load(self) -> DataFrame:
        """Loads data from filepath.

        Returns:
            Data from filepath as pyspark dataframe.
        """
        load_path = self._get_load_path()
        spark_load_path = to_spark_path(self._protocol, str(load_path))

        spark_session = get_spark_with_remote_support()

        reader = spark_session.read
        if self._schema:
            reader = reader.schema(self._schema)

        return (
            reader.format(self._file_format)
            .options(**self._load_args)
            .load(spark_load_path)
        )

    def _save(self, data: DataFrame | pd.DataFrame) -> None:
        """Saves pyspark dataframe.

        Args:
            data: PySpark DataFrame or Pandas DataFrame to save.
                  Pandas DataFrames will be automatically converted to Spark.
        """
        import pandas as pd  # noqa: PLC0415

        spark_session = get_spark_with_remote_support()

        # Convert Pandas DataFrame to Spark DataFrame if needed
        if isinstance(data, pd.DataFrame):
            data = spark_session.createDataFrame(data)

        save_path = self._get_save_path()
        spark_save_path = to_spark_path(self._protocol, str(save_path))

        # Create a copy of save_args to avoid mutation
        save_args = self._save_args.copy()

        # Extract mode and partitionBy (these are handled separately by Spark)
        mode = save_args.pop("mode", None)
        partition_by = save_args.pop("partitionBy", None)

        # Prepare writer
        writer = data.write

        if mode:
            writer = writer.mode(mode)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        # Save with remaining options
        writer.format(self._file_format).options(**save_args).save(spark_save_path)

    def _exists(self) -> bool:
        """Check if the dataset exists.

        Returns:
            True if the dataset exists, False otherwise.
        """
        load_path = self._get_load_path()
        spark_load_path = to_spark_path(self._protocol, str(load_path))

        try:
            spark_session = get_spark_with_remote_support()
            # Try to read the metadata without loading data
            spark_session.read.format(self._file_format).load(spark_load_path).schema
            return True
        except Exception as exc:
            # Check for specific error messages indicating non-existence
            error_msg = str(exc).lower()
            non_existence_indicators = [
                "path does not exist",
                "file not found",
                "is not a delta table",
                "no such file",
                "pathnotfoundexception",
                "filenotfoundexception",
            ]
            if any(msg in error_msg for msg in non_existence_indicators):
                return False
            # Re-raise for unexpected errors
            logger.warning(f"Error checking existence of {spark_load_path}: {exc}")
            raise

    def _describe(self) -> dict[str, Any]:
        """Describe the dataset configuration.

        Returns:
            Dictionary with dataset configuration details.
        """
        return {
            "filepath": to_spark_path(self._protocol, self._path),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _get_filesystem_ops(
        self, protocol: str, filepath: str, credentials: dict[str, Any]
    ) -> tuple:
        """Get filesystem operations for exists and glob.

        Args:
            protocol: Filesystem protocol.
            filepath: Original filepath.
            credentials: Credentials for filesystem access.

        Returns:
            Tuple of (exists_function, glob_function).
        """
        if protocol == "dbfs" and deployed_on_databricks():
            try:
                spark_session = get_spark_with_remote_support()
                dbutils = get_dbutils(spark_session)
                if dbutils:
                    logger.debug("Using optimised DBFS operations via dbutils")
                    return (
                        partial(dbfs_exists, dbutils=dbutils),
                        partial(dbfs_glob, dbutils=dbutils),
                    )
            except Exception as exc:
                logger.warning(f"Failed to get dbutils, falling back to fsspec: {exc}")

        # Regular fsspec for everything else
        fs = get_spark_filesystem(protocol, credentials)
        return fs.exists, fs.glob

    def _handle_delta_format(self) -> None:
        """Handle delta-specific configurations."""
        supported_modes = {"append", "overwrite", "error", "errorifexists", "ignore"}
        write_mode = self._save_args.get("mode")
        if (
            write_mode
            and self._file_format == "delta"
            and write_mode not in supported_modes
        ):
            raise DatasetError(
                f"It is not possible to perform 'save()' for file format 'delta' "
                f"with mode '{write_mode}' on 'SparkDataset'. "
                f"Please use 'spark.DeltaTableDataset' instead."
            )
