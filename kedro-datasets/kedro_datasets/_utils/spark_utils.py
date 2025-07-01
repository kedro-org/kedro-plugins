from typing import TYPE_CHECKING, Union

from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from databricks.connect import DatabricksSession


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
