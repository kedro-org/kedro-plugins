import json
import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Union

import fsspec
from kedro.io.core import DatasetError, get_protocol_and_path
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from databricks.connect import DatabricksSession
    from pyspark.sql.types import StructType

import logging

logger = logging.getLogger(__name__)


def get_spark() -> Union[SparkSession, "DatabricksSession"]:
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


def get_spark_with_remote_support() -> Union[SparkSession, "DatabricksSession"]:
    """Get Spark session with support for Spark Connect and Databricks Connect.

    Returns:
        SparkSession instance.
    """
    try:
        from pyspark.sql import SparkSession  # noqa: PLC0415
    except ImportError as exc:
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
        raise ImportError(msg) from exc

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
        except Exception as exc:
            logger.debug(f"Databricks Connect failed, falling back: {exc}")

    # Try Spark Connect (Spark 3.4+)
    if spark_remote := os.environ.get("SPARK_REMOTE"):
        try:
            logger.debug(f"Using Spark Connect: {spark_remote}")
            return SparkSession.builder.remote(spark_remote).getOrCreate()
        except Exception as exc:
            logger.debug(f"Spark Connect failed, falling back: {exc}")

    # Fall back to classic Spark session
    logger.debug("Using classic Spark session")
    return SparkSession.builder.getOrCreate()


def get_spark_filesystem(protocol: str, credentials: dict[str, Any] | None = None):
    """Get fsspec filesystem instance with Spark-specific mappings.

    Args:
        protocol: Filesystem protocol.
        credentials: Optional credentials for filesystem access.

    Returns:
        Filesystem instance.
    """
    credentials = credentials or {}

    # Map Spark protocols to fsspec protocols
    protocol_map = {
        "s3a": "s3",
        "s3n": "s3",
        "dbfs": "file",  # DBFS is mounted as local when using fsspec
        "": "file",
    }

    fsspec_protocol = protocol_map.get(protocol, protocol)

    try:
        return fsspec.filesystem(fsspec_protocol, **credentials)
    except ImportError as exc:
        # Provide targeted help for missing filesystem implementations
        error_msg = str(exc)
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
        raise ImportError(msg) from exc


def load_spark_schema_from_file(schema_config: dict[str, Any]) -> "StructType":
    """Load Spark schema from JSON file.

    Args:
        schema_config: Dictionary with 'filepath' and optional 'credentials'.

    Returns:
        StructType loaded from file.
    """
    filepath = schema_config.get("filepath")
    if not filepath:
        raise DatasetError(
            "Schema load argument does not specify a 'filepath' attribute. Please "
            "include a path to a JSON-serialised 'pyspark.sql.types.StructType'."
        )

    credentials = deepcopy(schema_config.get("credentials")) or {}
    protocol, schema_path = get_protocol_and_path(filepath)
    file_system = fsspec.filesystem(protocol, **credentials)
    pure_posix_path = PurePosixPath(schema_path)

    # Open schema file
    with file_system.open(str(pure_posix_path)) as fs_file:
        try:
            from pyspark.sql.types import StructType  # noqa: PLC0415

            return StructType.fromJson(json.loads(fs_file.read()))
        except Exception as exc:
            raise DatasetError(
                f"Contents of 'schema.filepath' ({schema_path}) are invalid. Please "
                f"provide a valid JSON-serialised 'pyspark.sql.types.StructType'."
            ) from exc
