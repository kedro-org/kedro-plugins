import os
from fnmatch import fnmatch
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Union

from kedro.io.core import get_protocol_and_path
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from databricks.connect import DatabricksSession
    from pyspark.dbutils import DBUtils

import logging

logger = logging.getLogger(__name__)


def parse_glob_pattern(pattern: str) -> str:
    special = ("*", "?", "[")
    clean = []
    for part in pattern.split("/"):
        if any(char in part for char in special):
            break
        clean.append(part)
    return "/".join(clean)


def split_filepath(filepath: str | os.PathLike) -> tuple[str, str]:
    split_ = str(filepath).split("://", 1)
    if len(split_) == 2:  # noqa: PLR2004
        return split_[0] + "://", split_[1]
    return "", split_[0]


def strip_dbfs_prefix(path: str, prefix: str = "/dbfs") -> str:
    return path[len(prefix) :] if path.startswith(prefix) else path


def dbfs_glob(pattern: str, dbutils: "DBUtils") -> list[str]:
    """Perform a custom glob search in DBFS using the provided pattern.
    It is assumed that version paths are managed by Kedro only.

    Args:
        pattern: Glob pattern to search for.
        dbutils: dbutils instance to operate with DBFS.

    Returns:
            List of DBFS paths prefixed with '/dbfs' that satisfy the glob pattern.
    """
    pattern = strip_dbfs_prefix(pattern)
    prefix = parse_glob_pattern(pattern)
    matched = set()
    filename = pattern.split("/")[-1]

    for file_info in dbutils.fs.ls(prefix):
        if file_info.isDir():
            path = str(
                PurePosixPath(strip_dbfs_prefix(file_info.path, "dbfs:")) / filename
            )
            if fnmatch(path, pattern):
                path = "/dbfs" + path
                matched.add(path)
    return sorted(matched)


def get_dbutils(spark: Union[SparkSession, "DatabricksSession"]) -> "DBUtils":
    """Get the instance of 'dbutils' or None if the one could not be found."""
    dbutils = globals().get("dbutils")
    if dbutils:
        return dbutils

    try:
        from pyspark.dbutils import DBUtils  # noqa: PLC0415

        dbutils = DBUtils(spark)
    except ImportError:
        try:
            import IPython  # noqa: PLC0415
        except ImportError:
            pass
        else:
            ipython = IPython.get_ipython()
            dbutils = ipython.user_ns.get("dbutils") if ipython else None

    return dbutils


def dbfs_exists(pattern: str, dbutils: "DBUtils") -> bool:
    """Perform an `ls` list operation in DBFS using the provided pattern.
    It is assumed that version paths are managed by Kedro.
    Broad `Exception` is present due to `dbutils.fs.ExecutionError` that
    cannot be imported directly.
    Args:
        pattern: Filepath to search for.
        dbutils: dbutils instance to operate with DBFS.
    Returns:
        Boolean value if filepath exists.
    """
    pattern = strip_dbfs_prefix(pattern)
    file = parse_glob_pattern(pattern)
    try:
        dbutils.fs.ls(file)
        return True
    except Exception:
        return False


def deployed_on_databricks() -> bool:
    """Check if running on Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def parse_spark_filepath(filepath: str) -> tuple[str, str]:
    """Parse filepath handling special cases like DBFS and Unity Catalog.

    Args:
        filepath: Path to parse.

    Returns:
        Tuple of (protocol, path).
    """
    # Handle DBFS paths
    if filepath.startswith("/dbfs/"):
        # /dbfs/path -> dbfs protocol with /path
        path = filepath[6:]  # Remove /dbfs prefix
        if not path.startswith("/"):
            path = "/" + path
        return "dbfs", path
    elif filepath.startswith("dbfs:/"):
        # dbfs:/path -> Fix: handle single slash DBFS format
        path = filepath[6:]  # Remove "dbfs:/"
        if not path.startswith("/"):
            path = "/" + path
        return "dbfs", path
    elif filepath.startswith("/Volumes"):
        # Unity Catalog volumes
        return "file", filepath
    else:
        # For standard protocols with ://
        return get_protocol_and_path(filepath)


def validate_databricks_path(filepath: str) -> None:
    """Warn about potential Databricks path issues.

    Args:
        filepath: Path to validate.
    """
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
            "Using SparkDataset on Databricks without the `/dbfs/`, `dbfs:/`, or `/Volumes` prefix "
            "in the filepath is a known source of error. You must add this prefix to %s",
            filepath,
        )


def to_spark_path(filepath: str, protocol: str, path: str) -> str:
    """Convert any path to Spark-compatible format.

    Args:
        filepath: Original filepath (used if protocol/path not provided).
        protocol: Detected protocol.
        path: Path component without protocol.

    Returns:
        Spark-compatible path string.
    """
    # For Databricks paths
    if protocol == "dbfs":
        # Ensure dbfs:/ format for Spark
        if not path.startswith("/"):
            path = "/" + path
        return f"dbfs:{path}"

    # Map to Spark protocols
    spark_protocols = {
        "s3": "s3a",  # Spark prefers s3a://
        "s3n": "s3a",
        "gs": "gs",
        "abfs": "abfs",
        "wasbs": "wasbs",
        "file": "file",
        "": "file",
    }

    spark_protocol = spark_protocols.get(protocol, protocol)

    # Handle local paths
    if spark_protocol == "file":
        # Unity Catalog volumes don't use file:// prefix
        if path.startswith("/Volumes"):
            return path
        # Regular local files need file:// prefix
        if not path.startswith("/"):
            path = f"/{path}"
        return f"file://{path}"
    elif not spark_protocol:
        return filepath  # Return original if we can't parse
    else:
        return f"{spark_protocol}://{path}"
