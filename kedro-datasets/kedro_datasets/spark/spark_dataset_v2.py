"""``AbstractVersionedDataset`` implementation to access Spark dataframes using
``pyspark``.
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from functools import partial
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_protocol_and_path,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

from kedro_datasets._utils.databricks_utils import (
    dbfs_exists,
    dbfs_glob,
    deployed_on_databricks,
    get_dbutils,
    strip_dbfs_prefix,
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
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets.spark import SparkDatasetV2
        >>> from pyspark.sql import SparkSession
        >>> from pyspark.sql.types import IntegerType, Row, StringType, StructField, StructType
        >>>
        >>> schema = StructType(
        ...     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
        ... )
        >>> data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]
        >>> spark_df = SparkSession.builder.getOrCreate().createDataFrame(data, schema)
        >>>
        >>> dataset = SparkDatasetV2(filepath="tmp_path/test_data")
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
    ):
        self.file_format = file_format
        self.load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self.save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self.credentials = credentials or {}
        self.metadata = metadata

        # Parse filepath
        self.protocol, self.path = self._parse_filepath(filepath)

        # Validate and warn about Databricks paths
        self._validate_databricks_path(filepath)

        # Get filesystem for metadata operations (exists, glob)
        exists_function, glob_function = self._get_filesystem_ops()

        # Store Spark compatible path for I/O
        self._spark_path = self._to_spark_path(filepath)

        # Handle schema if provided
        self._schema = self._load_schema_from_file(self.load_args.pop("schema", None))

        super().__init__(
            filepath=PurePosixPath(self.path),
            version=version,
            exists_function=exists_function,
            glob_function=glob_function,
        )

        self._validate_delta_format()

    def _parse_filepath(self, filepath: str) -> tuple[str, str]:
        """Parse filepath handling special cases like DBFS."""
        # Handle DBFS paths
        if filepath.startswith("/dbfs/"):
            # /dbfs/path -> dbfs protocol with /path
            return "dbfs", filepath[6:]  # Remove /dbfs prefix
        elif filepath.startswith("dbfs:/"):
            # dbfs:/path -> already in correct format
            return get_protocol_and_path(filepath)
        elif filepath.startswith("/Volumes"):
            # Unity Catalog volumes
            return "file", filepath
        else:
            return get_protocol_and_path(filepath)

    def _validate_databricks_path(self, filepath: str) -> None:
        """Warn about potential Databricks path issues."""
        if (
            deployed_on_databricks()
            and not (
                filepath.startswith("/dbfs")
                or filepath.startswith("dbfs:/")
                or filepath.startswith("/Volumes")
            )
            and not any(
                filepath.startswith(f"{p}://")
                for p in ["s3", "s3a", "s3n", "gs", "abfs", "wasbs"]
            )
        ):
            logger.warning(
                "Using SparkDatasetV2 on Databricks without the `/dbfs/`, `dbfs:/`, or `/Volumes` prefix "
                "in the filepath is a known source of error. You must add this prefix to %s",
                filepath,
            )

    def _get_filesystem_ops(self) -> tuple:
        """Get filesystem operations with DBFS optimization."""
        # Special handling for DBFS to avoid performance issues
        # This addresses the critical performance issue raised by deepyaman
        if self.protocol == "dbfs" and deployed_on_databricks():
            try:
                spark = self._get_spark()
                dbutils = get_dbutils(spark)
                if dbutils:
                    logger.debug("Using optimized DBFS operations via dbutils")
                    return (
                        partial(dbfs_exists, dbutils=dbutils),
                        partial(dbfs_glob, dbutils=dbutils),
                    )
            except Exception as e:
                logger.warning(f"Failed to get dbutils, falling back to fsspec: {e}")

        # Regular fsspec for everything else
        fs = self._get_filesystem()
        return fs.exists, fs.glob

    def _get_filesystem(self):
        """Get fsspec filesystem with helpful errors for missing deps."""
        try:
            import fsspec  # noqa: PLC0415
        except ImportError:
            raise ImportError(
                "fsspec is required for SparkDatasetV2. "
                "Install with: pip install fsspec"
            )

        # Map Spark protocols to fsspec protocols
        protocol_map = {
            "s3a": "s3",
            "s3n": "s3",
            "dbfs": "file",  # DBFS is mounted as local when using fsspec
            "": "file",
        }

        fsspec_protocol = protocol_map.get(self.protocol, self.protocol)

        try:
            return fsspec.filesystem(fsspec_protocol, **self.credentials)
        except ImportError as e:
            # Provide targeted help for missing filesystem implementations
            error_msg = str(e)
            if "s3fs" in error_msg:
                msg = (
                    "s3fs not installed. Install with:\n"
                    "  pip install 'kedro-datasets[spark-s3]' or\n"
                    "  pip install s3fs"
                )
            elif "gcsfs" in error_msg:
                msg = (
                    "gcsfs not installed. Install with:\n"
                    "  pip install 'kedro-datasets[spark-gcs]' or\n"
                    "  pip install gcsfs"
                )
            elif "adlfs" in error_msg or "azure" in error_msg:
                msg = (
                    "adlfs not installed for Azure. Install with:\n"
                    "  pip install 'kedro-datasets[spark-azure]' or\n"
                    "  pip install adlfs"
                )
            else:
                msg = f"Missing filesystem implementation: {error_msg}"
            raise ImportError(msg) from e

    def _to_spark_path(self, filepath: str) -> str:
        """Convert to Spark-compatible path format."""
        filepath = str(filepath)

        # Apply DBFS prefix stripping for consistency
        filepath = strip_dbfs_prefix(filepath)

        protocol, path = get_protocol_and_path(filepath)

        # Special handling for Databricks paths
        if self.protocol == "dbfs":
            # Ensure dbfs:/ format for Spark
            return f"dbfs:/{path}"

        # Map to Spark protocols
        spark_protocols = {
            "s3": "s3a",  # Spark prefers s3a://
            "gs": "gs",
            "abfs": "abfs",
            "wasbs": "wasbs",
            "file": "file",
            "": "file",
        }

        spark_protocol = spark_protocols.get(protocol, protocol)

        # Handle local paths
        if spark_protocol == "file":
            # Ensure absolute path for local files
            if not path.startswith("/"):
                path = f"/{path}"
            return f"file://{path}"
        elif not spark_protocol:
            return path
        else:
            return f"{spark_protocol}://{path}"

    def _get_spark(self) -> SparkSession:
        """Get Spark session with support for Spark Connect and Databricks Connect."""
        try:
            from pyspark.sql import SparkSession  # noqa: PLC0415
        except ImportError as e:
            # Detect environment and provide specific help
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                msg = (
                    "Cannot import PySpark on Databricks. This is usually a "
                    "databricks-connect conflict. Try:\n"
                    "  pip uninstall pyspark\n"
                    "  pip install databricks-connect"
                )
            elif "EMR_RELEASE_LABEL" in os.environ:
                msg = "PySpark should be pre-installed on EMR. Check your cluster configuration."
            else:
                msg = (
                    "PySpark not installed. Install based on your environment:\n"
                    "  Local: pip install 'kedro-datasets[spark-local]'\n"
                    "  Databricks: Use pre-installed Spark or databricks-connect\n"
                    "  Spark Connect: pip install 'kedro-datasets[spark-connect]'\n"
                    "  Cloud: Check your platform's Spark setup"
                )
            raise ImportError(msg) from e

        # Try Databricks Connect first (for remote development)
        if "DATABRICKS_HOST" in os.environ and "DATABRICKS_TOKEN" in os.environ:
            try:
                # Databricks Connect configuration
                logger.debug("Attempting to use Databricks Connect")
                builder = SparkSession.builder
                builder.remote(
                    f"sc://{os.environ['DATABRICKS_HOST']}:443/;token={os.environ['DATABRICKS_TOKEN']}"
                )
                return builder.getOrCreate()
            except Exception as e:
                logger.debug(f"Databricks Connect failed, falling back: {e}")

        # Try Spark Connect (Spark 3.4+)
        if spark_remote := os.environ.get("SPARK_REMOTE"):
            try:
                logger.debug(f"Using Spark Connect: {spark_remote}")
                return SparkSession.builder.remote(spark_remote).getOrCreate()
            except Exception as e:
                logger.debug(f"Spark Connect failed, falling back: {e}")

        # Fall back to classic Spark session
        logger.debug("Using classic Spark session")
        return SparkSession.builder.getOrCreate()

    @staticmethod
    def _load_schema_from_file(schema: dict[str, Any] | None) -> StructType | None:
        """Load schema from file if provided."""
        if schema is None:
            return None

        if not isinstance(schema, dict):
            # Assume it's already a StructType
            return schema

        filepath = schema.get("filepath")
        if not filepath:
            raise DatasetError(
                "Schema dict must have 'filepath' attribute. "
                "Please provide a path to a JSON-serialised 'pyspark.sql.types.StructType'."
            )

        try:
            import fsspec  # noqa: PLC0415
            from pyspark.sql.types import StructType  # noqa: PLC0415
        except ImportError as e:
            if "pyspark" in str(e):
                raise ImportError("PySpark required to process schema") from e
            raise ImportError("fsspec required for schema loading") from e

        protocol, path = get_protocol_and_path(filepath)
        fs = fsspec.filesystem(protocol, **schema.get("credentials", {}))

        try:
            with fs.open(path, "r") as f:
                schema_json = json.load(f)
            return StructType.fromJson(schema_json)
        except Exception as e:
            raise DatasetError(
                f"Failed to load schema from {filepath}. "
                f"Ensure it contains valid JSON-serialised StructType."
            ) from e

    def load(self) -> DataFrame:
        """Load data using Spark"""
        load_path = self._get_load_path()
        spark_load_path = self._to_spark_path(str(load_path))

        spark = self._get_spark()

        reader = spark.read
        if self._schema:
            reader = reader.schema(self._schema)

        return (
            reader.format(self.file_format)
            .options(**self.load_args)
            .load(spark_load_path)
        )

    def save(self, data: DataFrame) -> None:
        """Save data using Spark"""
        save_path = self._get_save_path()
        spark_save_path = self._to_spark_path(str(save_path))

        # Prepare writer
        writer = data.write

        # Apply mode if specified
        mode = self.save_args.pop("mode", None)
        if mode:
            writer = writer.mode(mode)

        # Apply partitioning if specified
        partition_by = self.save_args.pop("partitionBy", None)
        if partition_by:
            writer = writer.partitionBy(partition_by)

        # Save with format and options
        writer.format(self.file_format).options(**self.save_args).save(spark_save_path)

        # Restore save_args for potential reuse
        if mode:
            self.save_args["mode"] = mode
        if partition_by:
            self.save_args["partitionBy"] = partition_by

    def _exists(self) -> bool:
        """Check existence using Spark read attempt for better accuracy."""
        load_path = self._get_load_path()
        spark_load_path = self._to_spark_path(str(load_path))

        try:
            spark = self._get_spark()
            # Try to read the metadata without loading data
            spark.read.format(self.file_format).load(spark_load_path).schema
            return True
        except Exception as e:
            # Check for specific error messages indicating non-existence
            error_msg = str(e).lower()
            if any(
                msg in error_msg
                for msg in [
                    "path does not exist",
                    "file not found",
                    "is not a delta table",
                    "no such file",
                ]
            ):
                return False
            # Re-raise for unexpected errors
            logger.warning(f"Error checking existence of {spark_load_path}: {e}")
            raise

    def _validate_delta_format(self):
        """Validate Delta-specific configurations"""
        if self.file_format == "delta":
            mode = self.save_args.get("mode")
            supported = {"append", "overwrite", "error", "errorifexists", "ignore"}
            if mode and mode not in supported:
                raise DatasetError(
                    f"Delta format doesn't support mode '{mode}'. "
                    f"Use one of {supported} or DeltaTableDataset for advanced operations."
                )

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._spark_path,
            "file_format": self.file_format,
            "load_args": self.load_args,
            "save_args": self.save_args,
            "version": self._version,
            "protocol": self.protocol,
        }
