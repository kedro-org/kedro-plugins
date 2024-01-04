"""``AbstractDataset`` implementation to access Snowflake using Snowpark dataframes
"""
import logging
from copy import deepcopy
from typing import Any

import snowflake.snowpark as sp
from kedro.io.core import AbstractDataset, DatasetError

logger = logging.getLogger(__name__)


class SnowparkTableDataset(AbstractDataset):
    """``SnowparkTableDataset`` loads and saves Snowpark dataframes.

    As of Mar-2023, the snowpark connector only works with Python 3.8.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

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

    You can skip everything but "table_name" if the database and
    schema are provided via credentials. That way catalog entries can be shorter
    if, for example, all used Snowflake tables live in same database/schema.
    Values in the dataset definition take priority over those defined in credentials.

    Example:
    Credentials file provides all connection attributes, catalog entry
    "weather" reuses credentials parameters, "polygons" catalog entry reuses
    all credentials parameters except providing a different schema name.
    Second example of credentials file uses ``externalbrowser`` authentication.

    catalog.yml

    .. code-block:: yaml

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

    credentials.yml

    .. code-block:: yaml

        snowflake_client:
          account: 'ab12345.eu-central-1'
          port: 443
          warehouse: "datascience_wh"
          database: "detailed_data"
          schema: "observations"
          user: "service_account_abc"
          password: "supersecret"

    credentials.yml (with externalbrowser authenticator)

    .. code-block:: yaml

        snowflake_client:
          account: 'ab12345.eu-central-1'
          port: 443
          warehouse: "datascience_wh"
          database: "detailed_data"
          schema: "observations"
          user: "john_doe@wdomain.com"
          authenticator: "externalbrowser"

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
        schema: str = None,
        database: str = None,
        load_args: dict[str, Any] = None,
        save_args: dict[str, Any] = None,
        credentials: dict[str, Any] = None,
        metadata: dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SnowparkTableDataset``.

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
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._table_name = table_name
        self._database = database
        self._schema = schema

        connection_parameters = credentials
        connection_parameters.update(
            {"database": self._database, "schema": self._schema}
        )
        self._connection_parameters = connection_parameters

        self.metadata = metadata

    def _describe(self) -> dict[str, Any]:
        return {
            "table_name": self._table_name,
            "database": self._database,
            "schema": self._schema,
        }

    @staticmethod
    def _get_session(connection_parameters) -> sp.Session:
        """Given a connection string, create singleton connection
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
            session = sp.context.get_active_session()
        except sp.exceptions.SnowparkSessionException:
            logger.debug("No active snowpark session found. Creating...")
            session = sp.Session.builder.configs(connection_parameters).create()
        return session

    @property
    def _session(self) -> sp.Session:
        return self._get_session(self._connection_parameters)

    def _load(self) -> sp.DataFrame:
        table_name = [
            self._database,
            self._schema,
            self._table_name,
        ]

        sp_df = self._session.table(".".join(table_name))
        return sp_df

    def _save(self, data: sp.DataFrame) -> None:
        table_name = [
            self._database,
            self._schema,
            self._table_name,
        ]

        data.write.save_as_table(table_name, **self._save_args)

    def _exists(self) -> bool:
        session = self._session
        query = "SELECT COUNT(*) FROM {database}.INFORMATION_SCHEMA.TABLES \
                WHERE TABLE_SCHEMA = '{schema}' \
                AND TABLE_NAME = '{table_name}'"
        rows = session.sql(
            query.format(
                database=self._database,
                schema=self._schema,
                table_name=self._table_name,
            )
        ).collect()
        return rows[0][0] == 1
