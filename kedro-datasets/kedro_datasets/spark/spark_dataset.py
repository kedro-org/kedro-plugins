"""``AbstractVersionedDataset`` implementation to access Spark dataframes using
``pyspark``.
"""

from __future__ import annotations

import logging
import os
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

logger = logging.getLogger(__name__)


class SparkDataset(AbstractVersionedDataset):
    """``SparkDataset`` loads and saves Spark dataframes.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

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

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

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
        >>> dataset = SparkDataset(filepath="tmp_path/test_data")
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
        self.load_args = load_args or {}
        self.save_args = save_args or {}
        self.credentials = credentials or {}
        self.metadata = metadata

        # Parse filepath
        self.protocol, self.path = get_protocol_and_path(filepath)

        # Get filesystem for metadata operations (exists, glob)
        self._fs = self._get_filesystem()

        # Store Spark compatible path for I/O
        self._spark_path = self._to_spark_path(filepath)

        # Handle schema if provided
        self._schema = self._process_schema(self.load_args.pop("schema", None))

        super().__init__(
            filepath=PurePosixPath(self.path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        self._validate_delta_format()

    def _get_filesystem(self):
        """Get fsspec filesystem with helpful errors for missing deps"""
        try:
            import fsspec  # noqa: PLC0415
        except ImportError:
            raise ImportError("fsspec is required")

        # Normalise protocols
        protocol_map = {
            "s3a": "s3",
            "s3n": "s3",  # Spark S3 variants
            "dbfs": "file",  # DBFS is mounted as local
            "": "file",  # Default to local
        }

        fsspec_protocol = protocol_map.get(self.protocol, self.protocol)

        try:
            return fsspec.filesystem(fsspec_protocol, **self.credentials)
        except ImportError as e:
            # Provide targeted help
            if "s3fs" in str(e):
                msg = "s3fs not installed. Install with: pip install 'kedro-datasets[spark-s3]'"
            elif "gcsfs" in str(e):
                msg = "gcsfs not installed. Install with: pip install gcsfs"
            elif "adlfs" in str(e):
                msg = "adlfs not installed. Install with: pip install adlfs"
            else:
                msg = str(e)
            raise ImportError(msg) from e

    def _to_spark_path(self, filepath: str) -> str:
        """Convert to Spark-compatible path format"""
        filepath = str(filepath)  # Convert PosixPath to string
        protocol, path = get_protocol_and_path(filepath)

        # Handle special cases
        if filepath.startswith("/dbfs/"):
            # Databricks: /dbfs/path -> dbfs:/path
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                return "dbfs:/" + filepath[6:]
            return filepath

        # Map to Spark protocols
        spark_protocols = {
            "s3": "s3a",  # Critical: Spark prefers s3a://
            "gs": "gs",
            "abfs": "abfs",
            "file": "",  # Local paths don't need protocol
            "": "",
        }

        spark_protocol = spark_protocols.get(protocol, protocol)

        if not spark_protocol:
            return path
        return f"{spark_protocol}://{path}"

    def _get_spark(self) -> SparkSession:
        """Lazy load Spark with environment specific guidance"""
        try:
            from pyspark.sql import SparkSession  # noqa: PLC0415

            return SparkSession.builder.getOrCreate()
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
                    "  Cloud: Check your platform's Spark setup"
                )
            raise ImportError(msg) from e

    def _process_schema(self, schema: Any) -> Any:
        """Process schema argument if provided"""
        if schema is None:
            return None

        if isinstance(schema, dict):
            # Load from file
            schema_path = schema.get("filepath")
            if not schema_path:
                raise DatasetError("Schema dict must have 'filepath'")

            # Use fsspec to load
            import json  # noqa: PLC0415

            protocol, path = get_protocol_and_path(schema_path)

            try:
                import fsspec  # noqa: PLC0415

                fs = fsspec.filesystem(protocol, **schema.get("credentials", {}))
                with fs.open(path, "r") as f:
                    schema_json = json.load(f)

                # Lazy import StructType
                from pyspark.sql.types import StructType  # noqa: PLC0415

                return StructType.fromJson(schema_json)
            except ImportError as e:
                if "pyspark" in str(e):
                    raise ImportError("PySpark required to process schema") from e
                raise
            except Exception as e:
                raise DatasetError(f"Failed to load schema from {schema_path}") from e

        return schema

    def load(self) -> DataFrame:
        """Load data using Spark"""
        spark = self._get_spark()

        reader = spark.read
        if self._schema:
            reader = reader.schema(self._schema)

        return (
            reader.format(self.file_format)
            .options(**self.load_args)
            .load(self._spark_path)
        )

    def save(self, data: DataFrame) -> None:
        """Save data using Spark"""
        writer = data.write

        if mode := self.save_args.pop("mode", None):
            writer = writer.mode(mode)

        if partition_by := self.save_args.pop("partitionBy", None):
            writer = writer.partitionBy(partition_by)

        writer.format(self.file_format).options(**self.save_args).save(self._spark_path)

    def _exists(self) -> bool:
        """Existence check using fsspec"""
        try:
            return self._fs.exists(self.path)
        except Exception:
            # Fallback to Spark check for special cases (e.g., Delta tables)
            if self.file_format == "delta":
                try:
                    spark = self._get_spark()
                    spark.read.format("delta").load(self._spark_path)
                    return True
                except Exception:
                    return False
            return False

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
        }
