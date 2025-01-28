import copy
import datetime as dt
from pathlib import PurePosixPath
import re
from typing import Any, NoReturn

import fsspec
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_filepath_str,
    get_protocol_and_path,
)
import polars as pl
from sqlalchemy import MetaData, Table, create_engine, inspect, select
from sqlalchemy.exc import NoSuchModuleError

from kedro_datasets._typing import TablePreview



KNOWN_PIP_INSTALL = {
    "psycopg2": "psycopg2",
    "mysqldb": "mysqlclient",
    "cx_Oracle": "cx_Oracle",
    "mssql": "pyodbc",
}

DRIVER_ERROR_MESSAGE = """
A module/driver is missing when connecting to your SQL server. SQLDataset
 supports SQLAlchemy drivers. Please refer to
 https://docs.sqlalchemy.org/core/engines.html#supported-databases
 for more information.
\n\n
"""


def _find_known_drivers(module_import_error: ImportError) -> str | None:
    """Looks up known keywords in a ``ModuleNotFoundError`` so that it can
    provide better guideline for the user.

    Args:
        module_import_error: Error raised while connecting to a SQL server.

    Returns:
        Instructions for installing missing driver. An empty string is
        returned in case error is related to an unknown driver.

    """

    # module errors contain string "No module name 'module_name'"
    # we are trying to extract module_name surrounded by quotes here
    res = re.findall(r"'(.*?)'", str(module_import_error.args[0]).lower())

    # in case module import error does not match our expected pattern
    # we have no recommendation
    if not res:
        return None

    missing_module = res[0]

    if KNOWN_PIP_INSTALL.get(missing_module):
        return (
            f"You can also try installing missing driver with\n"
            f"\npip install {KNOWN_PIP_INSTALL.get(missing_module)}"
        )

    return None


def _get_missing_module_error(import_error: ImportError) -> DatasetError:
    missing_module_instruction = _find_known_drivers(import_error)

    if missing_module_instruction is None:
        return DatasetError(
            f"{DRIVER_ERROR_MESSAGE}Loading failed with error:\n\n{str(import_error)}"
        )

    return DatasetError(f"{DRIVER_ERROR_MESSAGE}{missing_module_instruction}")


def _get_sql_alchemy_missing_error() -> DatasetError:
    return DatasetError(
        "The SQL dialect in your connection is not supported by "
        "SQLAlchemy. Please refer to "
        "https://docs.sqlalchemy.org/core/engines.html#supported-databases "
        "for more information."
    )


class PolarsDatabaseDataset(AbstractDataset[None, pl.DataFrame]):

    def __init__(  # noqa: PLR0913
        self,
        sql: str | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        filepath: str | None = None,
        table_name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``PolarsDatabaseDataset``."""
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

        if not (credentials and "con" in credentials and credentials["con"]):
            raise DatasetError(
                "'con' argument cannot be empty. Please "
                "provide a SQLAlchemy connection string."
            )

        default_load_args: dict[str, Any] = {}

        self._load_args = (
            {**default_load_args, **load_args}
            if load_args is not None
            else default_load_args
        )

        self.table_name = table_name
        self.metadata = metadata

        # load sql query from file
        if sql:
            self._load_args["sql"] = sql
            self._filepath = None
        else:
            # filesystem for loading sql file
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            protocol, path = get_protocol_and_path(str(filepath))

            self._protocol = protocol
            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)
            self._filepath = path
        self._connection_str = credentials["con"]
        self._connection_args = {
            k: credentials[k] for k in credentials.keys() if k != "con"
        }
        if "mssql" in self._connection_str:
            self.adapt_mssql_date_params()

    @classmethod
    def create_connection(
        cls, connection_str: str, connection_args: dict | None = None
    ) -> None:
        """Given a connection string, create singleton connection
        to be used across all instances of `PolarsDatabaseDataset` that
        need to connect to the same source.
        """
        connection_args = connection_args or {}
        try:
            engine = create_engine(connection_str, **connection_args)
        except ImportError as import_error:
            raise _get_missing_module_error(import_error) from import_error
        except NoSuchModuleError as exc:
            raise _get_sql_alchemy_missing_error() from exc

        cls.engines[connection_str] = engine

    @property
    def engine(self):
        """The ``Engine`` object for the dataset's connection string."""
        cls = type(self)

        if self._connection_str not in cls.engines:
            self.create_connection(self._connection_str, self._connection_args)

        return cls.engines[self._connection_str]

    def load(self) -> pl.DataFrame:
        load_args = copy.deepcopy(self._load_args)

        if self._filepath:
            load_path = get_filepath_str(PurePosixPath(self._filepath), self._protocol)
            with self._fs.open(load_path, mode="r") as fs_file:
                query = fs_file.read()
        else:
            query = load_args.pop("sql")

        return pl.read_database(
            query=query,
            connection=self._connection_str,
            **load_args
        )

    def save(self, data: pl.DataFrame) -> NoReturn:
        if not self.table_name:
            raise DatasetError(
                "'table_name' argument is required to save datasets."
            )
        
        data.write_database(
            table=self.table_name,
            connection=self._connection_str,
        )

    def _exists(self) -> bool:
        insp = inspect(self.engine)
        schema = self._load_args.get("schema", None)
        return insp.has_table(self._load_args["table_name"], schema)

    def preview(self, nrows: int = 5) -> TablePreview:
        """
        Generate a preview of the dataset with a specified number of rows.

        Args:
            nrows: The number of rows to include in the preview. Defaults to 5.

        Returns:
            dict: A dictionary containing the data in a split format.
        """
        table_name = self._load_args["table_name"]

        metadata = MetaData()
        table_ref = Table(table_name, metadata, autoload_with=self.engine)

        query = select(table_ref).limit(nrows)  # type: ignore[arg-type]

        with self.engine.connect() as conn:
            result = conn.execute(query)
            data_preview = pl.DataFrame(result.fetchall(), columns=result.keys())

        preview_data = data_preview.to_dict(orient="split")
        return preview_data
    
    # For mssql only
    def adapt_mssql_date_params(self) -> None:
        """We need to change the format of datetime parameters.
        MSSQL expects datetime in the exact format %y-%m-%dT%H:%M:%S.
        Here, we also accept plain dates.
        `pyodbc` does not accept named parameters, they must be provided as a list."""
        params = self._load_args.get("params", [])
        if not isinstance(params, list):
            raise DatasetError(
                "Unrecognized `params` format. It can be only a `list`, "
                f"got {type(params)!r}"
            )
        new_load_args = []
        for value in params:
            try:
                as_date = dt.date.fromisoformat(value)
                new_val = dt.datetime.combine(as_date, dt.time.min)
                new_load_args.append(new_val.strftime("%Y-%m-%dT%H:%M:%S"))
            except (TypeError, ValueError):
                new_load_args.append(value)
        if new_load_args:
            self._load_args["params"] = tuple(new_load_args)