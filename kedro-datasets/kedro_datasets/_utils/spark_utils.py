"""Utility functions for Spark."""
from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Union

import fsspec
from kedro.io.core import DatasetError, get_protocol_and_path
from pyspark.sql import SparkSession

# Databricks Connect: only imported at runtime if available
try:
    from databricks.connect import DatabricksSession
except ImportError:
    DatabricksSession = None  # allows graceful fallback

if TYPE_CHECKING:
    from databricks.connect import DatabricksSession
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def get_spark() -> SparkSession | DatabricksSession:
    """
    Returns the SparkSession. In case databricks-connect is available we use it for
    extended configuration mechanisms and notebook compatibility,
    otherwise we use classic pyspark.
    """
    try:
        # When using databricks-connect >= 13.0.0 (a.k.a databricks-connect-v2)
        # the remote session is instantiated using the databricks module
        # If the databricks-connect module is installed, we use a remote session
        from databricks.connect import DatabricksSession  # noqa: PLC0415

        # We can't test this as there's no Databricks test env available
        spark = DatabricksSession.builder.getOrCreate()  # pragma: no cover

    except ImportError:
        # For "normal" spark sessions that don't use databricks-connect
        # we get spark normally
        spark = SparkSession.builder.getOrCreate()

    return spark


def get_spark_with_remote_support() -> SparkSession | DatabricksSession:
    """Get Spark session with support for Spark Connect and Databricks Connect.

    This function attempts to create a Spark session in the following order:
    1. Databricks Connect (if DATABRICKS_HOST and DATABRICKS_TOKEN are set)
    2. Spark Connect (if SPARK_REMOTE is set)
    3. Classic local Spark session

    Returns:
        SparkSession instance.

    Raises:
        ImportError: If PySpark is not installed with helpful installation hints.
    """
    try:
        from pyspark.sql import SparkSession  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover
        # Detect environment and provide specific help
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:  # pragma: no cover
            msg = (  # pragma: no cover
                "Cannot import PySpark on Databricks. This is usually a "
                "databricks-connect conflict. Try:\n"
                "  pip uninstall pyspark\n"
                "  pip install databricks-connect"
            )
        elif "EMR_RELEASE_LABEL" in os.environ:  # pragma: no cover
            msg = "PySpark should be pre-installed on EMR. Check your cluster configuration."
        else:  # pragma: no cover
            msg = (
                "PySpark not installed. Install based on your environment:\n"
                "  Local: pip install 'kedro-datasets[spark-local]'\n"
                "  Databricks: Use pre-installed Spark or databricks-connect\n"
                "  Spark Connect: pip install 'kedro-datasets[spark-connect]'\n"
                "  Cloud: Check your platform's Spark setup"
            )
        raise ImportError(msg) from exc  # pragma: no cover

    # Try Databricks Connect first (for remote development)
    if "DATABRICKS_HOST" in os.environ and "DATABRICKS_TOKEN" in os.environ:
        if DatabricksSession is not None:
            try:
                logger.debug("Attempting to use Databricks Connect")
                return DatabricksSession.builder.serverless(True).getOrCreate()
            except Exception as exc:
                logger.debug(f"Databricks Connect failed, falling back: {exc}")
        else:
            logger.debug(
                "DATABRICKS_HOST and DATABRICKS_TOKEN are set but "
                "databricks-connect is not installed"
            )

    # Try Spark Connect (Spark 3.4+)
    spark_remote = os.environ.get("SPARK_REMOTE")
    if spark_remote:
        try:
            logger.debug(f"Using Spark Connect: {spark_remote}")
            return SparkSession.builder.remote(spark_remote).getOrCreate()
        except Exception as exc:
            logger.debug(f"Spark Connect failed, falling back: {exc}")

    # Fall back to classic Spark session
    logger.debug("Using classic Spark session")
    return SparkSession.builder.getOrCreate()


def get_spark_filesystem(
    protocol: str, credentials: dict[str, Any] | None = None
) -> fsspec.AbstractFileSystem:
    """Get fsspec filesystem instance with Spark-specific protocol mappings.

    Args:
        protocol: Filesystem protocol (e.g., 's3a', 'gs', 'abfs').
        credentials: Optional credentials for filesystem access.

    Returns:
        Filesystem instance.

    Raises:
        ImportError: If required filesystem package is not installed.
    """
    credentials = credentials or {}

    # Map Spark protocols to fsspec protocols
    protocol_map = {
        "s3a": "s3",
        "s3n": "s3",
        "s3": "s3",
        "gs": "gcs",
        "gcs": "gcs",
        "abfs": "abfs",
        "abfss": "abfs",
        "wasbs": "abfs",
        "wasb": "abfs",
        "hdfs": "hdfs",
        "dbfs": "file",  # DBFS is mounted as local filesystem when using fsspec
        "file": "file",
        "": "file",
    }

    fsspec_protocol = protocol_map.get(protocol, protocol)

    try:
        return fsspec.filesystem(fsspec_protocol, **credentials)
    except ImportError as exc:
        # Provide targeted help for missing filesystem implementations
        error_msg = str(exc).lower()

        if "s3fs" in error_msg or fsspec_protocol == "s3":
            msg = (
                "s3fs not installed. Install with:\n"
                "  pip install 'kedro-datasets[spark-s3]' or\n"
                "  pip install s3fs"
            )
        elif "gcsfs" in error_msg or fsspec_protocol == "gcs":
            msg = (
                "gcsfs not installed. Install with:\n"
                "  pip install 'kedro-datasets[spark-gcs]' or\n"
                "  pip install gcsfs"
            )
        elif "adlfs" in error_msg or "azure" in error_msg or fsspec_protocol == "abfs":
            msg = (
                "adlfs not installed for Azure. Install with:\n"
                "  pip install 'kedro-datasets[spark-azure]' or\n"
                "  pip install adlfs"
            )
        elif "hdfs" in error_msg or fsspec_protocol == "hdfs":
            msg = (
                "HDFS support requires PyArrow. Install with:\n"
                "  pip install 'kedro-datasets[spark-hdfs]' or\n"
                "  pip install pyarrow"
            )
        else:
            msg = f"Missing filesystem implementation for '{protocol}': {error_msg}"

        raise ImportError(msg) from exc


def load_spark_schema_from_file(schema_config: dict[str, Any]) -> StructType:
    """Load Spark schema from JSON file.

    Args:
        schema_config: Dictionary with 'filepath' key pointing to a JSON file
            containing a serialized StructType, and optional 'credentials' key.

    Returns:
        StructType loaded from file.

    Raises:
        DatasetError: If filepath is not specified or file contents are invalid.
    """
    filepath = schema_config.get("filepath")
    if not filepath:
        raise DatasetError(
            "Schema load argument does not specify a 'filepath' attribute. Please "
            "include a path to a JSON-serialised 'pyspark.sql.types.StructType'."
        )

    credentials = deepcopy(schema_config.get("credentials")) or {}
    protocol, schema_path = get_protocol_and_path(filepath)

    # Normalise empty protocol
    if not protocol:
        protocol = "file"  # pragma: no cover

    file_system = fsspec.filesystem(protocol, **credentials)
    pure_posix_path = PurePosixPath(schema_path)

    # Open and parse schema file
    with file_system.open(str(pure_posix_path)) as fs_file:
        try:
            from pyspark.sql.types import StructType  # noqa: PLC0415

            schema_json = json.loads(fs_file.read())
            return StructType.fromJson(schema_json)
        except json.JSONDecodeError as exc:
            raise DatasetError(
                f"Contents of 'schema.filepath' ({schema_path}) are invalid JSON. "
                f"Please provide a valid JSON-serialised 'pyspark.sql.types.StructType'."
            ) from exc
        except Exception as exc:
            raise DatasetError(
                f"Contents of 'schema.filepath' ({schema_path}) are invalid. Please "
                f"provide a valid JSON-serialised 'pyspark.sql.types.StructType'."
            ) from exc
