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
        from databricks.connect import DatabricksSession

        # We can't test this as there's no Databricks test env available
        try:
            spark = DatabricksSession.builder.getOrCreate()  # pragma: no cover
        # this can't be narrowed down since databricks-connect throws error of Exception type
        except Exception as e:
            error_message = str(e)
            if (
                error_message
                == "Cluster id or serverless are required but were not specified."
            ):
                raise type(e)(
                    "DatabricksSession is expected to behave as singleton but it didn't. "
                    "Either set up DATABRICKS_CONFIG_PROFILE or DATABRICKS_PROFILE and DATABRICKS_SERVERLESS_COMPUTE_ID "
                    "env variables in your hooks prior to using the spark session. "
                    "Read more about these variables here: "
                    "https://docs.databricks.com/aws/en/dev-tools/databricks-connect/cluster-config#config-profile-env-var"
                ) from e
            pass

    except ImportError:
        # For "normal" spark sessions that don't use databricks-connect
        # we get spark normally
        spark = SparkSession.builder.getOrCreate()

    return spark
