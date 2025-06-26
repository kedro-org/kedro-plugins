"""``AbstractVersionedDataset`` implementation to access Spark dataframes using
``pyspark``.
"""

from __future__ import annotations

import json
import logging
from copy import deepcopy
from fnmatch import fnmatch
from functools import partial
from pathlib import PurePosixPath
from typing import Any
from warnings import warn

import fsspec
from hdfs import HdfsError, InsecureClient
from kedro.io.core import (
    CLOUD_PROTOCOLS,
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException
from s3fs import S3FileSystem

from kedro_datasets._utils.databricks_utils import (
    dbfs_exists,
    dbfs_glob,
    deployed_on_databricks,
    get_dbutils,
    parse_glob_pattern,
    split_filepath,
    strip_dbfs_prefix,
)
from kedro_datasets._utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class KedroHdfsInsecureClient(InsecureClient):
    """Subclasses ``hdfs.InsecureClient`` and implements ``hdfs_exists``
    and ``hdfs_glob`` methods required by ``SparkDataset``"""

    def hdfs_exists(self, hdfs_path: str) -> bool:
        """Determines whether given ``hdfs_path`` exists in HDFS.

        Args:
            hdfs_path: Path to check.

        Returns:
            True if ``hdfs_path`` exists in HDFS, False otherwise.
        """
        return bool(self.status(hdfs_path, strict=False))

    def hdfs_glob(self, pattern: str) -> list[str]:
        """Perform a glob search in HDFS using the provided pattern.

        Args:
            pattern: Glob pattern to search for.

        Returns:
            List of HDFS paths that satisfy the glob pattern.
        """
        prefix = parse_glob_pattern(pattern) or "/"
        matched = set()
        try:
            for dpath, _, fnames in self.walk(prefix):
                if fnmatch(dpath, pattern):
                    matched.add(dpath)
                matched |= {
                    f"{dpath}/{fname}"
                    for fname in fnames
                    if fnmatch(f"{dpath}/{fname}", pattern)
                }
        except HdfsError:  # pragma: no cover
            # HdfsError is raised by `self.walk()` if prefix does not exist in HDFS.
            # Ignore and return an empty list.
            pass
        return sorted(matched)


class SparkDataset(AbstractVersionedDataset[DataFrame, DataFrame]):
    """``SparkDataset`` loads and saves Spark dataframes.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        weather:
          type: spark.SparkDataset
          filepath: s3a://your_bucket/data/01_raw/weather/*
          file_format: csv
          load_args:
            header: True
            inferSchema: True
          save_args:
            sep: '|'
            header: True

        weather_with_schema:
          type: spark.SparkDataset
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
          type: spark.SparkDataset
          filepath: data/02_intermediate/data.parquet
          file_format: parquet
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from kedro_datasets.spark import SparkDataset
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> dataset = SparkDataset(filepath=tmp_path / "test_data")
        >>> dataset.save(spark_df)
        >>> reloaded = dataset.load()
        >>> assert Row(name="Bob", age=12) in reloaded.take(4)

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
        """Creates a new instance of ``SparkDataset``.

        Args:
            filepath: Filepath in POSIX format to a Spark dataframe. When using Databricks
                specify ``filepath``s starting with ``/dbfs/``.
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
            credentials: Credentials to access the S3 bucket, such as
                ``key``, ``secret``, if ``filepath`` prefix is ``s3a://`` or ``s3n://``.
                Optional keyword arguments passed to ``hdfs.client.InsecureClient``
                if ``filepath`` prefix is ``hdfs://``. Ignored otherwise.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        credentials = deepcopy(credentials) or {}
        fs_prefix, filepath = split_filepath(filepath)
        path = PurePosixPath(filepath)
        exists_function = None
        glob_function = None
        self.metadata = metadata

        if (
            not (filepath.startswith("/dbfs") or filepath.startswith("/Volumes"))
            and fs_prefix not in (protocol + "://" for protocol in CLOUD_PROTOCOLS)
            and deployed_on_databricks()
        ):
            logger.warning(
                "Using SparkDataset on Databricks without the `/dbfs/` or `/Volumes` prefix in the "
                "filepath is a known source of error. You must add this prefix to %s",
                filepath,
            )
        if fs_prefix and fs_prefix in ("s3a://"):
            _s3 = S3FileSystem(**credentials)
            exists_function = _s3.exists
            # Ensure cache is not used so latest version is retrieved correctly.
            glob_function = partial(_s3.glob, refresh=True)

        elif fs_prefix == "hdfs://":
            if version:
                warn(
                    f"HDFS filesystem support for versioned {self.__class__.__name__} is "
                    f"in beta and uses 'hdfs.client.InsecureClient', please use with "
                    f"caution"
                )

            # default namenode address
            credentials.setdefault("url", "http://localhost:9870")
            credentials.setdefault("user", "hadoop")

            _hdfs_client = KedroHdfsInsecureClient(**credentials)
            exists_function = _hdfs_client.hdfs_exists
            glob_function = _hdfs_client.hdfs_glob  # type: ignore

        elif filepath.startswith("/dbfs/"):
            # dbfs add prefix to Spark path by default
            # See https://github.com/kedro-org/kedro-plugins/issues/117
            dbutils = get_dbutils(get_spark())
            if dbutils:
                glob_function = partial(dbfs_glob, dbutils=dbutils)
                exists_function = partial(dbfs_exists, dbutils=dbutils)
        else:
            filesystem = fsspec.filesystem(fs_prefix.strip("://"), **credentials)
            exists_function = filesystem.exists
            glob_function = filesystem.glob

        super().__init__(
            filepath=path,
            version=version,
            exists_function=exists_function,
            glob_function=glob_function,
        )

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        # Handle schema load argument
        self._schema = self._load_args.pop("schema", None)
        if self._schema is not None:
            if isinstance(self._schema, dict):
                self._schema = self._load_schema_from_file(self._schema)

        self._file_format = file_format
        self._fs_prefix = fs_prefix
        self._handle_delta_format()

    @staticmethod
    def _load_schema_from_file(schema: dict[str, Any]) -> StructType:
        filepath = schema.get("filepath")
        if not filepath:
            raise DatasetError(
                "Schema load argument does not specify a 'filepath' attribute. Please"
                "include a path to a JSON-serialised 'pyspark.sql.types.StructType'."
            )

        credentials = deepcopy(schema.get("credentials")) or {}
        protocol, schema_path = get_protocol_and_path(filepath)
        file_system = fsspec.filesystem(protocol, **credentials)
        pure_posix_path = PurePosixPath(schema_path)
        load_path = get_filepath_str(pure_posix_path, protocol)

        # Open schema file
        with file_system.open(load_path) as fs_file:
            try:
                return StructType.fromJson(json.loads(fs_file.read()))
            except Exception as exc:
                raise DatasetError(
                    f"Contents of 'schema.filepath' ({schema_path}) are invalid. Please"
                    f"provide a valid JSON-serialised 'pyspark.sql.types.StructType'."
                ) from exc

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def load(self) -> DataFrame:
        load_path = strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))
        read_obj = get_spark().read

        # Pass schema if defined
        if self._schema:
            read_obj = read_obj.schema(self._schema)

        return read_obj.load(load_path, self._file_format, **self._load_args)

    def save(self, data: DataFrame) -> None:
        save_path = strip_dbfs_prefix(self._fs_prefix + str(self._get_save_path()))
        data.write.save(save_path, self._file_format, **self._save_args)

    def _exists(self) -> bool:
        load_path = strip_dbfs_prefix(self._fs_prefix + str(self._get_load_path()))

        try:
            get_spark().read.load(load_path, self._file_format)
        except AnalysisException as exception:
            # `AnalysisException.desc` is deprecated with pyspark >= 3.4
            message = exception.desc if hasattr(exception, "desc") else str(exception)
            if "Path does not exist:" in message or "is not a Delta table" in message:
                return False
            raise
        return True

    def _handle_delta_format(self) -> None:
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
