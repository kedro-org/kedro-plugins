"""``GBQTableDataset`` loads and saves data from/to Google BigQuery. It uses pandas-gbq
to read and write from/to BigQuery table.
"""

from __future__ import annotations

import copy
from pathlib import PurePosixPath
from typing import Any, ClassVar, NoReturn

import fsspec
import pandas as pd
import pandas_gbq as pd_gbq
from google.auth.credentials import Credentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_filepath_str,
    get_protocol_and_path,
    validate_on_forbidden_chars,
)

from kedro_datasets._utils import ConnectionMixin


def _get_credentials(credentials: dict[str, Any] | str) -> ServiceAccountCredentials:
    # If dict: Assume it's a service account json
    if isinstance(credentials, dict):
        return ServiceAccountCredentials.from_service_account_info(credentials)

    # If str: Assume it's a path to a service account key json file
    return ServiceAccountCredentials.from_service_account_file(credentials)


class GBQTableDataset(ConnectionMixin, AbstractDataset[None, pd.DataFrame]):
    """``GBQTableDataset`` loads and saves data from/to Google BigQuery.
    It uses pandas-gbq to read and write from/to BigQuery table.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        vehicles:
          type: pandas.GBQTableDataset
          dataset: big_query_dataset
          table_name: big_query_table
          project: my-project
          credentials: gbq-creds
          load_args:
            reauth: True
          save_args:
            chunk_size: 100
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import pandas as pd
        >>> from kedro_datasets.pandas import GBQTableDataset
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = GBQTableDataset(
        ...     dataset="dataset", table_name="table_name", project="my-project"
        >>> )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"progress_bar": False}

    _CONNECTION_GROUP: ClassVar[str] = "bigquery"

    def __init__(  # noqa: PLR0913
        self,
        *,
        dataset: str,
        table_name: str,
        project: str | None = None,
        credentials: dict[str, Any] | str | Credentials | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``GBQTableDataset``.

        Args:
            dataset: Google BigQuery dataset.
            table_name: Google BigQuery table name.
            project: Google BigQuery Account project ID.
                Optional when available from the environment.
                https://cloud.google.com/resource-manager/docs/creating-managing-projects
            credentials: Credentials for accessing Google APIs.
                Either a credential that bases on ``google.auth.credentials.Credentials`` OR
                a service account json as a dictionary OR
                a path to a service account key json file.
                https://googleapis.dev/python/google-auth/latest/
            load_args: Pandas options for loading BigQuery table into DataFrame.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_gbq.html
                All defaults are preserved.
            save_args: Pandas options for saving DataFrame to BigQuery table.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_gbq.html
                All defaults are preserved, but "progress_bar", which is set to False.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: When ``load_args['location']`` and ``save_args['location']``
                are different.
        """
        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        self._validate_location()
        validate_on_forbidden_chars(dataset=dataset, table_name=table_name)

        self._dataset = dataset
        self._table_name = table_name
        self._project_id = project

        if (not isinstance(credentials, Credentials)) and (credentials is not None):
            credentials = _get_credentials(credentials)

        self._connection_config = {
            "project": self._project_id,
            "credentials": credentials,
            "location": self._save_args.get("location"),
        }

        self.metadata = metadata

    def _describe(self) -> dict[str, Any]:
        return {
            "dataset": self._dataset,
            "table_name": self._table_name,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def _connect(self) -> bigquery.Client:
        return bigquery.Client(
            project=self._connection_config["project"],
            credentials=self._connection_config["credentials"],
            location=self._connection_config["location"],
        )

    def load(self) -> pd.DataFrame:
        sql = f"select * from {self._dataset}.{self._table_name}"  # nosec
        self._load_args.setdefault("query_or_table", sql)
        return pd_gbq.read_gbq(
            project_id=self._project_id,
            credentials=self._connection._credentials,
            **self._load_args,
        )

    def save(self, data: pd.DataFrame) -> None:
        pd_gbq.to_gbq(
            dataframe=data,
            destination_table=f"{self._dataset}.{self._table_name}",
            project_id=self._project_id,
            credentials=self._connection._credentials,
            **self._save_args,
        )

    def _exists(self) -> bool:
        table_ref = self._connection.dataset(self._dataset).table(self._table_name)
        try:
            self._connection.get_table(table_ref)
            return True
        except NotFound:
            return False

    def _validate_location(self):
        save_location = self._save_args.get("location")
        load_location = self._load_args.get("location")

        if save_location != load_location:
            raise DatasetError(
                """"load_args['location']" is different from "save_args['location']". """
                "The 'location' defines where BigQuery data is stored, therefore has "
                "to be the same for save and load args. "
                "Details: https://cloud.google.com/bigquery/docs/locations"
            )


class GBQQueryDataset(AbstractDataset[None, pd.DataFrame]):
    """``GBQQueryDataset`` loads data from a provided SQL query from Google
    BigQuery. It uses ``pandas_gbq.read_gbq`` which itself uses ``pandas-gbq``
    internally to read from BigQuery table. Therefore it supports all allowed
    pandas options on ``read_gbq``.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

    ```yaml

    vehicles:
        type: pandas.GBQQueryDataset
        sql: "select shuttle, shuttle_id from spaceflights.shuttles;"
        project: my-project
        credentials: gbq-creds
        load_args:
        reauth: True
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

    ```python
    from kedro_datasets.pandas import GBQQueryDataset

    sql = "SELECT * FROM dataset_1.table_a"

    dataset = GBQQueryDataset(sql, project="my-project")

    sql_data = dataset.load()
    ```
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        sql: str | None = None,
        project: str | None = None,
        credentials: dict[str, Any] | Credentials | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        filepath: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``GBQQueryDataset``.

        Args:
            sql: The sql query statement.
            project: Google BigQuery Account project ID.
                Optional when available from the environment.
                https://cloud.google.com/resource-manager/docs/creating-managing-projects
            credentials: Credentials for accessing Google APIs.
                Either a credential that bases on ``google.auth.credentials.Credentials`` OR
                a service account json as a dictionary OR
                a path to a service account key json file.
                https://googleapis.dev/python/google-auth/latest/
            load_args: Pandas options for loading BigQuery table into DataFrame.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_gbq.html
                All defaults are preserved.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``) used for reading the
                SQL query from filepath.
            filepath: A path to a file with a sql query statement.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: When ``sql`` and ``filepath`` parameters are either both empty
                or both provided, as well as when the `save()` method is invoked.
        """
        if sql and filepath:
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be provided."
                "Please only provide one."
            )

        if not (sql or filepath):
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be empty."
                "Please provide a sql query or path to a sql query file."
            )

        # Handle default load arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}

        self._project_id = project

        if (not isinstance(credentials, Credentials)) and (credentials is not None):
            self._credentials = _get_credentials(credentials)
        else:
            self._credentials = credentials

        # load sql query from arg or from file
        if sql:
            self._load_args["query_or_table"] = sql
            self._filepath = None
        else:
            # filesystem for loading sql file
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            protocol, path = get_protocol_and_path(str(filepath))

            self._protocol = protocol
            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)
            self._filepath = path

        self.metadata = metadata

    def _describe(self) -> dict[str, Any]:
        load_args = copy.deepcopy(self._load_args)
        desc = {}
        desc["sql"] = str(load_args.pop("query_or_table", None))
        desc["filepath"] = str(self._filepath)
        desc["load_args"] = str(load_args)

        return desc

    def load(self) -> pd.DataFrame:
        load_args = copy.deepcopy(self._load_args)

        if self._filepath:
            load_path = get_filepath_str(PurePosixPath(self._filepath), self._protocol)
            with self._fs.open(load_path, mode="r") as fs_file:
                load_args["query_or_table"] = fs_file.read()

        return pd_gbq.read_gbq(
            project_id=self._project_id,
            credentials=self._credentials,
            **load_args,
        )

    def save(self, data: None) -> NoReturn:
        raise DatasetError("'save' is not supported on GBQQueryDataset")
