"""``AbstractDataSet`` implementation to access Snowflake using Snowpark dataframes
"""
from copy import deepcopy
from typing import Any, Dict, Union

import pandas as pd
import snowflake.snowpark as sp

from kedro.io.core import AbstractDataSet, DataSetError


# TODO: Update docstring after interface finalised
# TODO: Add to docs example of using API to add dataset
class SnowParkDataSet(
    AbstractDataSet[pd.DataFrame, pd.DataFrame]
):
    """``SnowParkDataSet`` loads and saves Snowpark dataframes.

    Example adding a catalog entry with
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        >>> weather:
        >>>   type: snowflake.SnowParkDataSet
        >>>   table_name: weather_data
        >>>   warehouse: warehouse_warehouse
        >>>   database: meteorology
        >>>   schema: observations
        >>>   credentials: db_credentials
        >>>   load_args (WIP):
        >>>     Do we need any?
        >>>   save_args:
        >>>     mode: overwrite
    """

    # this dataset cannot be used with ``ParallelRunner``,
    # therefore it has the attribute ``_SINGLE_PROCESS = True``
    # for parallelism within a Spark pipeline please consider
    # ``ThreadRunner`` instead
    _SINGLE_PROCESS = True
    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    # TODO: Update docstring
    def __init__(  # pylint: disable=too-many-arguments
        self,
        table_name: str,
        warehouse: str,
        database: str,
        schema: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        credentials: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``SnowParkDataSet``.

        Args:
            table_name:
            warehouse:
            database:
            schema:
            load_args:
            save_args: whatever supported by snowpark writer
            https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrameWriter.saveAsTable.html
            credentials:
        """

        if not table_name:
            raise DataSetError("'table_name' argument cannot be empty.")

        # TODO: Check if we can use default warehouse when user is not providing one explicitly
        if not warehouse:
            raise DataSetError("'warehouse' argument cannot be empty.")

        if not database:
            raise DataSetError("'database' argument cannot be empty.")

        if not schema:
            raise DataSetError("'schema' argument cannot be empty.")

        if not credentials:
            raise DataSetError("'credentials' argument cannot be empty.")

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._table_name = table_name
        self._warehouse = warehouse
        self._database = database
        self._schema = schema
        self._connection_parameters = {
            "account": credentials["account"],
            "user": credentials["user"],
            "password": credentials["password"],
            "warehouse": self._warehouse,
            "database": self._database,
            "schema": self._schema,
        }
        self._session = self._get_session(self._connection_parameters)

    def _describe(self) -> Dict[str, Any]:
        return dict(
            table_name=self._table_name,
            warehouse=self._warehouse,
            database=self._database,
            schema=self._schema,
        )

    # TODO: Do we want to make it static method?
    @staticmethod
    def _get_session(connection_parameters) -> sp.Session:
        """Given a connection string, create singleton connection
        to be used across all instances of `SnowParkDataSet` that
        need to connect to the same source.
            connection_params = {
                "account": "",
                "user": "",
                "password": "",
                "role": "", (optional)
                "warehouse": "", (optional)
                "database": "", (optional)
                "schema": "" (optional)
                }
        """
        try:
            # if hook is implemented, get active session
            session = sp.context.get_active_session()
        except sp.exceptions.SnowparkSessionException as exc:
            # create session if there is no active one
            session = sp.Session.builder.configs(connection_parameters).create()
        return session

    def _load(self) -> sp.DataFrame:
        table_name = [
            self._database,
            self._schema,
            self._table_name,
        ]

        sp_df = self._session.table(".".join(table_name))
        return sp_df

    def _save(self, data: Union[pd.DataFrame, sp.DataFrame]) -> None:
        if not isinstance(data, sp.DataFrame):
            sp_df = self._session.create_dataframe(data)
        else:
            sp_df = data

        table_name = [
            self._database,
            self._schema,
            self._table_name,
        ]

        sp_df.write.save_as_table(table_name, **self._save_args)

    def _exists(self) -> bool:
        session = self._session
        schema = self._schema
        exists = self._table_name in session.table_names(schema)
        return exists
