"""``AbstractDataset`` implementation to access Snowflake using Snowpark dataframes"""

from __future__ import annotations

import logging
from typing import Any, cast

import pandas as pd
from kedro.io.core import AbstractDataset, DatasetError
from snowflake.snowpark import DataFrame, Session
from snowflake.snowpark import context as sp_context
from snowflake.snowpark import exceptions as sp_exceptions

logger = logging.getLogger(__name__)


class SnowparkTableDataset(AbstractDataset):
    """``SnowparkTableDataset`` loads and saves Snowpark DataFrames.

    As of October 2024, the Snowpark connector works with Python 3.9, 3.10, and 3.11.
    Python 3.12 is not supported yet.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        weather:
          type: kedro_datasets.snowflake.SnowparkTableDataset
          table_name: "weather_data"
          database: "meteorology"
          schema: "observations"
          credentials: db_credentials
          save_args:
            mode: overwrite
            column_order: name
            table_type: ''
        ```

        You can skip everything but "table_name" if the database and schema are
        provided via credentials. This allows catalog entries to be shorter when
        all Snowflake tables are in the same database and schema. Values in the
        dataset definition take priority over those defined in credentials.

        The credentials file provides all connection attributes. The catalog entry
        for "weather" reuses the credentials parameters, while the "polygons" catalog
        entry reuses all credentials parameters except for specifying a different
        schema. The second example demonstrates the use of ``externalbrowser`` authentication.

        catalog.yml:

        ```yaml
        weather:
          type: kedro_datasets.snowflake.SnowparkTableDataset
          table_name: "weather_data"
          database: "meteorology"
          schema: "observations"
          credentials: snowflake_client
          save_args:
            mode: overwrite
            column_order: name
            table_type: ''

        polygons:
          type: kedro_datasets.snowflake.SnowparkTableDataset
          table_name: "geopolygons"
          credentials: snowflake_client
          schema: "geodata"
        ```

        credentials.yml:

        ```yaml

        snowflake_client:
          account: 'ab12345.eu-central-1'
          port: 443
          warehouse: "datascience_wh"
          database: "detailed_data"
          schema: "observations"
          user: "service_account_abc"
          password: "supersecret"
        ```

        credentials.yml (with externalbrowser authentication):

        ```yaml
        snowflake_client:
          account: 'ab12345.eu-central-1'
          port: 443
          warehouse: "datascience_wh"
          database: "detailed_data"
          schema: "observations"
          user: "john_doe@wdomain.com"
          authenticator: "externalbrowser"
        ```

    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a pipeline please consider
    # ``ThreadRunner`` instead
    _SINGLE_PROCESS = True
    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        table_name: str,
        schema: str | None = None,
        database: str | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        session: Session | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Creates a new instance of ``SnowparkTableDataset``.

        Args:
            table_name: The table name to load or save data to.
            schema: Name of the schema where ``table_name`` is.
                Optional as can be provided as part of ``credentials``
                dictionary. Argument value takes priority over one provided
                in ``credentials`` if any.
            database: Name of the database where ``schema`` is.
                Optional as can be provided as part of ``credentials``
                dictionary. Argument value takes priority over one provided
                in ``credentials`` if any.
            load_args: Currently not used
            save_args: Provided to underlying snowpark ``save_as_table``
                To find all supported arguments, see here:
                https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrameWriter.saveAsTable.html
            credentials: A dictionary with a snowpark connection string.
                To find all supported arguments, see here:
                https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """

        if not table_name:
            raise DatasetError("'table_name' argument cannot be empty.")

        if not credentials:
            raise DatasetError("'credentials' argument cannot be empty.")

        if not database:
            if not ("database" in credentials and credentials["database"]):
                raise DatasetError(
                    "'database' must be provided by credentials or dataset."
                )
            database = credentials["database"]

        if not schema:
            if not ("schema" in credentials and credentials["schema"]):
                raise DatasetError(
                    "'schema' must be provided by credentials or dataset."
                )
            schema = credentials["schema"]

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

        self._table_name = table_name
        self._database = database
        self._schema = schema

        connection_parameters = credentials
        connection_parameters.update(
            {"database": self._database, "schema": self._schema}
        )
        self._connection_parameters = connection_parameters
        self._session = session

        self.metadata = metadata

    def _describe(self) -> dict[str, Any]:
        return {
            "table_name": self._table_name,
            "database": self._database,
            "schema": self._schema,
        }

    @staticmethod
    def _get_session(connection_parameters) -> Session:
        """
        Given a connection string, create singleton connection
        to be used across all instances of `SnowparkTableDataset` that
        need to connect to the same source.
        connection_parameters is a dictionary of any values
        supported by snowflake python connector:
                https://docs.snowflake.com/en/user-guide/python-connector-api.html#connect
            example:
            connection_parameters = {
                "account": "",
                "user": "",
                "password": "", (optional)
                "role": "", (optional)
                "warehouse": "", (optional)
                "database": "", (optional)
                "schema": "", (optional)
                "authenticator: "" (optional)
                }
        """
        try:
            logger.debug("Trying to reuse active snowpark session...")
            session = sp_context.get_active_session()
        except sp_exceptions.SnowparkSessionException:
            logger.debug("No active snowpark session found. Creating...")
            session = Session.builder.configs(connection_parameters).create()
        return session

    @property
    def session(self) -> Session:
        """
        Retrieve or create a session.
        Returns:
            Session: The current session associated with the object.
        """
        if not self._session:
            self._session = self._get_session(self._connection_parameters)
        return self._session

    def load(self) -> DataFrame:
        """
        Load data from a specified database table.

        Returns:
            DataFrame: The loaded data as a Snowpark DataFrame.
        """
        return self.session.table(self._validate_and_get_table_name())

    def save(self, data: pd.DataFrame | DataFrame) -> None:
        """
        Check if the data is a Snowpark DataFrame or a Pandas DataFrame,
        convert it to a Snowpark DataFrame if needed, and save it to the specified table.

        Args:
            data (pd.DataFrame | DataFrame): The data to save.
        """
        if isinstance(data, pd.DataFrame):
            snowpark_df = self.session.create_dataframe(data)
        elif isinstance(data, DataFrame):
            snowpark_df = data
        else:
            raise DatasetError(
                f"Data of type {type(data)} is not supported for saving."
            )

        snowpark_df.write.save_as_table(
            self._validate_and_get_table_name(), **self._save_args
        )

    def _exists(self) -> bool:
        """
        Check if a specified table exists in the database.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            self.session.table(
                f"{self._database}.{self._schema}.{self._table_name}"
            ).show()
            return True
        except Exception as e:
            logger.debug(f"Table {self._table_name} does not exist: {e}")
            return False

    def _validate_and_get_table_name(self) -> str:
        """
        Validate that all parts of the table name are not None and join them into a string.

        Args:
            parts (list[str | None]): The list containing database, schema, and table name.

        Returns:
            str: The joined table name in the format 'database.schema.table'.

        Raises:
            ValueError: If any part of the table name is None.
        """
        parts: list[str | None] = [self._database, self._schema, self._table_name]
        if any(part is None or part == "" for part in parts):
            raise DatasetError("Database, schema or table name cannot be None or empty")
        parts_str = cast(list[str], parts)  # make linting happy
        return ".".join(parts_str)
