"""``AbstractDataset`` implementation to access Spark dataframes using
``pyspark``.
"""

from __future__ import annotations

import base64
import json
import logging
from copy import deepcopy
from typing import Any, NoReturn

from kedro.io import AbstractDataset, DatasetError
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame

from kedro_datasets._utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class GBQQueryDataset(AbstractDataset[None, DataFrame]):
    """``GBQQueryDataset`` loads data from Google BigQuery with a SQL query using BigQuery Spark connector.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        my_gbq_spark_data:
          type: spark.GBQQueryDataset
          sql: |
            SELECT * FROM your_table
          materialization_dataset: your_dataset
          materialization_project: your_project
          credentials:
            file: /path/to/your/credentials.json

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.spark import GBQQueryDataset
        >>> import pyspark.sql as sql
        >>>
        >>> # Define your SQL query
        >>> sql = "SELECT * FROM your_table"
        >>>
        >>> # Initialize dataset
        >>> dataset = GBQQueryDataset(
        ...     sql=sql,
        ...     materialization_dataset="your_dataset",
        ...     materialization_project="your_project",  # optional
        ...     credentials=dict(file="/path/to/your/credentials.json"),
        ... )
        >>>
        >>> # Load data
        >>> df = dataset.load()
        >>>
        >>> # Example output
        >>> df.show()
    """

    _VALID_CREDENTIALS_KEYS = {"base64", "file", "json"}

    def __init__(  # noqa: PLR0913
        self,
        sql: str,
        materialization_dataset: str,
        materialization_project: str | None = None,
        load_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``SparkGBQDataSet`` pointing to a specific table in Google BigQuery.

        Args:
            sql: SQL query to execute.
            materialization_dataset: The name of the dataset to materialize the query results.
            materialization_project: The name of the project to materialize the query results.
                Optional (defaults to the project id set by the credentials).
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
            credentials: Credentials to authenticate spark session with google bigquery.
                Dictionary with key specifying the type of credentials ('base64', 'file', 'json').
                Read more here:
                    https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#how-do-i-authenticate-outside-gce--dataproc
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._sql = sql
        self._materialization_dataset = materialization_dataset
        self._materialization_project = materialization_project
        self._load_args = load_args or {}
        self._credentials = credentials or {}
        self._metadata = metadata

    def _get_spark_credentials(self) -> dict[str, str]:
        if not self._credentials:
            return {}

        if len(self._credentials) > 1:
            raise ValueError(
                "Please provide only one of 'base64', 'file' or 'json' key in the credentials. "
                f"You provided: {list(self._credentials.keys())}"
            )
        if self._credentials.get("base64"):
            return {
                "credentials": self._credentials["base64"],
            }
        if self._credentials.get("file"):
            return {
                "credentialsFile": self._credentials["file"],
            }
        if self._credentials.get("json"):
            creds_b64 = base64.b64encode(
                json.dumps(self._credentials["json"]).encode("utf-8")
            ).decode("utf-8")
            return {"credentials": creds_b64}

        raise ValueError(
            f"Please provide one of 'base64', 'file' or 'json' key in the credentials. You provided: {list(self._credentials.keys())[0]}"
        )

    def _get_spark_load_args(self) -> dict[str, Any]:
        spark_load_args = deepcopy(self._load_args)
        spark_load_args["query"] = self._sql
        spark_load_args["materializationDataset"] = self._materialization_dataset

        if self._materialization_project:
            spark_load_args["materializationProject"] = self._materialization_project

        spark_load_args.update(self._get_spark_credentials())

        try:
            views_enabled_spark_conf = get_spark().conf.get("viewsEnabled")
        except Py4JJavaError:
            views_enabled_spark_conf = "false"

        if views_enabled_spark_conf != "true":
            spark_load_args["viewsEnabled"] = "true"
            logger.warning(
                "The 'viewsEnabled' configuration is not set to 'true' in the SparkSession. "
                "This is required for the Spark BigQuery connector to read via a SQL query. "
                "Setting 'viewsEnabled' to 'true' for the current query read operation. "
                "This may incur additional costs!"
            )

        return spark_load_args

    def load(self) -> DataFrame:
        """Loads data from Google BigQuery.

        Returns:
            A Spark DataFrame.
        """
        spark = get_spark()
        read_obj = spark.read.format("bigquery")

        return read_obj.load(**self._get_spark_load_args())

    def save(self, data: None) -> NoReturn:
        raise DatasetError("'save' is not supported on GBQQueryDataset")

    def _describe(self) -> dict[str, Any]:
        return {
            "sql": self._sql,
            "materialization_dataset": self._materialization_dataset,
            "materialization_project": self._materialization_project,
            "load_args": self._load_args,
            "metadata": self._metadata,
        }
