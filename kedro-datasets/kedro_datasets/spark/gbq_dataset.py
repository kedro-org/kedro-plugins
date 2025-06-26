"""``AbstractDataset`` implementation to access Spark dataframes using
BigQuery Spark connector .
"""

from __future__ import annotations

import base64
import json
import logging
from copy import deepcopy
from typing import Any, NoReturn

import fsspec
from kedro.io import AbstractDataset, DatasetError
from kedro.io.core import get_protocol_and_path
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame

from kedro_datasets._utils.spark_utils import get_spark

logger = logging.getLogger(__name__)


class GBQQueryDataset(AbstractDataset[None, DataFrame]):
    """``GBQQueryDataset`` loads data from Google BigQuery with a SQL query using BigQuery Spark connector.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        my_gbq_spark_data:
          type: spark.GBQQueryDataset
          sql: |
            SELECT * FROM your_table
          materialization_dataset: your_dataset
          materialization_project: your_project
          bq_credentials:
            file: /path/to/your/credentials.json
          fs_credentials:
            key: value
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from kedro_datasets.spark import GBQQueryDataset
        >>>
        >>> # Define your SQL query
        >>> sql = "SELECT * FROM your_table"
        >>>
        >>> # Initialize dataset
        >>> dataset = GBQQueryDataset(
        ...     sql=sql,
        ...     materialization_dataset="your_dataset",
        ...     materialization_project="your_project",  # optional
        ...     bq_credentials=dict(file="/path/to/your/credentials.json"),  # optional
        ...     fs_credentials=dict(key="value"),  # optional
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
        materialization_dataset: str,
        sql: str | None = None,
        filepath: str | None = None,
        materialization_project: str | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        bq_credentials: dict[str, Any] | None = None,
        fs_credentials: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``SparkGBQDataSet`` pointing to a specific table in Google BigQuery.

        Args:
            materialization_dataset: The name of the dataset to materialize the query results.
            sql: The SQL query to execute
            filepath: A path to a file with a sql query statement.
            materialization_project: The name of the project to materialize the query results.
                Optional (defaults to the project id set by the credentials).
            load_args: Load args passed to Spark DataFrameReader load method.
                It is dependent on the selected file format. You can find
                a list of read options for each supported format
                in Spark DataFrame read documentation:
                https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
            bq_credentials: Credentials to authenticate spark session with google bigquery.
                Dictionary with key specifying the type of credentials ('base64', 'file', 'json').
                Alternatively, you can pass the credentials in load_args as follows:

                When passing as `base64`:
                `load_args={"credentials": "your_credentials"}`

                When passing as a `file`:
                `load_args={"credentialsFile": "/path/to/your/credentials.json"}`

                When passing as a json object:
                This is not supported when passed directly in load_args. You can pass the json object (as a python dictionary) in the credentials
                by specifying the key as `json`.

                Read more here:
                    https://github.com/GoogleCloudDataproc/spark-bigquery-connector?tab=readme-ov-file#how-do-i-authenticate-outside-gce--dataproc
            fs_credentials: Credentials to authenticate with the filesystem.
                The keyword args would be directly passed to fsspec.filesystem constructor.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        if sql and filepath:
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be provided. "
                "Please only provide one."
            )

        if not (sql or filepath):
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be empty. "
                "Please provide a sql query or path to a sql query file."
            )

        if sql:
            self._sql = sql
            self._filepath = None
        else:
            protocol, path = get_protocol_and_path(str(filepath))

            self._fs_args = fs_args or {}
            self._fs_credentials = fs_credentials or {}
            self._fs_protocol = protocol

            self._fs = fsspec.filesystem(
                self._fs_protocol, **self._fs_credentials, **self._fs_args
            )
            self._filepath = path
            self._sql = None  # type: ignore

        self._materialization_dataset = materialization_dataset
        self._materialization_project = materialization_project
        self._load_args = load_args or {}
        self._bq_credentials = bq_credentials or {}

        self._metadata = metadata

    def _get_spark_bq_credentials(self) -> dict[str, str]:
        if not self._bq_credentials:
            return {}

        if len(self._bq_credentials) > 1:
            raise ValueError(
                f"Please provide only one of {self._VALID_CREDENTIALS_KEYS} key in the credentials. "
                f"You provided: {list(self._bq_credentials.keys())}"
            )
        if self._bq_credentials.get("base64"):
            return {
                "credentials": self._bq_credentials["base64"],
            }
        if self._bq_credentials.get("file"):
            return {
                "credentialsFile": self._bq_credentials["file"],
            }
        if self._bq_credentials.get("json"):
            creds_b64 = base64.b64encode(
                json.dumps(self._bq_credentials["json"]).encode("utf-8")
            ).decode("utf-8")
            return {"credentials": creds_b64}

        raise ValueError(
            f"Please provide one of {self._VALID_CREDENTIALS_KEYS} key in the credentials. You provided: {list(self._bq_credentials.keys())[0]}"
        )

    def _load_sql_from_filepath(self) -> str:
        with self._fs.open(self._filepath, "r") as f:
            return f.read()

    def _get_sql(self) -> str:
        if self._sql:
            return self._sql
        else:
            return self._load_sql_from_filepath()

    def _get_spark_load_args(self) -> dict[str, Any]:
        spark_load_args = deepcopy(self._load_args)
        spark_load_args["query"] = self._get_sql()
        spark_load_args["materializationDataset"] = self._materialization_dataset

        if self._materialization_project:
            spark_load_args["materializationProject"] = self._materialization_project

        spark_load_args.update(self._get_spark_bq_credentials())

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
