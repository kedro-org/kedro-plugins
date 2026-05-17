"""``PolarsDatabaseDataset`` to load and save data to a SQL backend using Polars."""

import copy
import re
from pathlib import PurePosixPath
from typing import Any, NoReturn

import fsspec
import polars as pl
from kedro.io.core import (
    AbstractDataset,
    DatasetError,
    get_filepath_str,
    get_protocol_and_path,
)
from sqlalchemy import create_engine
from sqlalchemy.exc import NoSuchModuleError

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
    """``PolarsDatabaseDataset`` loads data from a provided SQL query or write data to a table.

    It supports all allowed polars options on ``read_database`` and ``write_database``.
    Since Polars uses SQLAlchemy behind the scenes, when instantiating ``PolarsDatabaseDataset`` one needs to pass
    a compatible connection string either in ``credentials`` (see the example
    code snippet below) or in ``load_args``. Connection string formats supported
    by SQLAlchemy can be found here:
    https://docs.sqlalchemy.org/core/engines.html#database-urls

    Provide at least one of ``sql``, ``filepath``, or ``table_name`` (``sql``
    and ``filepath`` are mutually exclusive). ``load`` uses ``sql`` or
    ``filepath`` when given, otherwise ``SELECT * FROM <table_name>``.
    ``save`` always writes to ``table_name``.

    Schema-qualified tables can be passed directly to ``table_name`` using the
    ``schema_name.table_name`` form; there is no separate ``schema`` argument.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

    Load-and-save against a single table:

    ```yaml
    shuttles_table:
        type: polars.PolarsDatabaseDataset
        table_name: shuttles
        credentials: db_credentials
    ```

    Load via a custom (here schema-qualified) SQL query:

    ```yaml
    shuttle_id_dataset:
        type: polars.PolarsDatabaseDataset
        sql: "SELECT shuttle, shuttle_id FROM spaceflights.shuttles;"
        credentials: db_credentials
    ```

    Pass extra arguments to the underlying polars methods via ``load_args`` and
    ``save_args``:

    ```yaml
    shuttles_table:
        type: polars.PolarsDatabaseDataset
        table_name: shuttles
        credentials: db_credentials
        load_args:
            batch_size: 10000
            schema_overrides:
                shuttle_id: Int64
        save_args:
            if_table_exists: append
    ```

    Sample database credentials entry in ``credentials.yml``:

    ```
    db_credentials:
        con: postgresql://scott:tiger@localhost/test  # pragma: allowlist secret
        pool_size: 10 # additional parameters
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):
    ```python
    >>> from pathlib import Path
    >>> import polars as pl
    >>> import sqlite3
    >>>
    >>> from kedro_datasets_experimental.polars import PolarsDatabaseDataset
    >>>
    >>> data = pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
    >>> sql = "SELECT * FROM table_a"
    >>> tmp_path = Path.cwd() / "tmp"
    >>> tmp_path.mkdir(parents=True, exist_ok=True)
    >>> credentials = {"con": f"sqlite:///{tmp_path / 'test.db'}"}
    >>> dataset = PolarsDatabaseDataset(sql=sql, credentials=credentials, table_name="table_a")
    >>>
    >>> dataset.save(data)
    >>> reloaded = dataset.load()
    >>>
    >>> assert data.equals(reloaded)
    ```
    """
    # using Any because of Sphinx but it should be
    # sqlalchemy.engine.Engine or sqlalchemy.engine.base.Engine
    engines: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        sql: str | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        filepath: str | None = None,
        table_name: str | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``PolarsDatabaseDataset``.

        Args:
            sql: SQL query to execute on load. Mutually exclusive with
                ``filepath``. If neither is given, ``table_name`` must be
                provided and the dataset will load via ``SELECT * FROM <table_name>``.
            credentials: A dictionary with a ``SQLAlchemy`` connection string.
                Users are supposed to provide the connection string ``con``
                through credentials. To find all supported connection string
                formats, see here:
                https://docs.sqlalchemy.org/core/engines.html#database-urls
                Additional parameters for the sqlalchemy engine can be provided
                alongside the ``con`` parameter.
            load_args: Provided to the underlying ``polars.read_database``
                function along with the connection. To find all supported
                arguments, see here:
                https://docs.pola.rs/api/python/stable/reference/api/polars.read_database.html
            fs_args: Extra arguments passed to ``fsspec`` when ``filepath``
                points to a SQL file (e.g. credentials for remote storage).
            filepath: Path to a ``.sql`` file containing the query to execute
                on load. Mutually exclusive with ``sql``.
            table_name: Target table for ``save``. When ``sql`` and
                ``filepath`` are not provided, also used as the load source via
                ``SELECT * FROM <table_name>``. Schema-qualified tables can be
                passed as ``schema_name.table_name``.
            save_args: Provided to the underlying ``polars.DataFrame.write_database``
                method along with the connection string. To find all supported
                arguments, see here:
                https://docs.pola.rs/api/python/stable/reference/api/polars.DataFrame.write_database.html
                Defaults to ``{"if_table_exists": "replace"}`` — writes overwrite
                the target table unless overridden (e.g. ``if_table_exists: append``).
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: When both ``sql`` and ``filepath`` are provided, when
                none of ``sql``/``filepath``/``table_name`` is provided, or
                when ``con`` is missing from ``credentials``.
        """
        if sql and filepath:
            raise DatasetError(
                "'sql' and 'filepath' arguments cannot both be provided. "
                "Please only provide one."
            )

        if not (sql or filepath or table_name):
            raise DatasetError(
                "Provide at least one of 'sql', 'filepath', or 'table_name'. "
                "When only 'table_name' is given, the dataset will load the whole table."
            )

        if not (credentials and "con" in credentials and credentials["con"]):
            raise DatasetError(
                "'con' argument cannot be empty. Please "
                "provide a SQLAlchemy connection string."
            )

        default_load_args: dict[str, Any] = {}
        default_save_args: dict[str, Any] = {
            "if_table_exists": "replace"
        }

        self._load_args = (
            {**default_load_args, **load_args}
            if load_args is not None
            else default_load_args
        )

        self.table_name = table_name
        self._save_args = (
            {**default_save_args, **save_args}
            if save_args is not None
            else default_save_args
        )

        self.metadata = metadata

        if sql:
            self._load_args["sql"] = sql
            self._filepath = None
        elif filepath:
            # filesystem for loading sql file
            _fs_args = copy.deepcopy(fs_args) or {}
            _fs_credentials = _fs_args.pop("credentials", {})
            protocol, path = get_protocol_and_path(str(filepath))

            self._protocol = protocol
            self._fs = fsspec.filesystem(self._protocol, **_fs_credentials, **_fs_args)
            self._filepath = path
        else:
            # table_name-only mode: load() will build "SELECT * FROM <table_name>".
            self._filepath = None
        self._connection_str = credentials["con"]
        self._connection_args = {
            k: credentials[k] for k in credentials.keys() if k != "con"
        }

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

    def _describe(self) -> dict[str, Any]:
        load_args = copy.deepcopy(self._load_args)
        return {
            "sql": str(load_args.pop("sql", None)),
            "filepath": str(self._filepath),
            "load_args": str(load_args),
            "table_name": self.table_name,
            "save_args": str(self._save_args),
        }

    def load(self) -> pl.DataFrame:
        load_args = copy.deepcopy(self._load_args)

        if self._filepath:
            load_path = get_filepath_str(PurePosixPath(self._filepath), self._protocol)
            with self._fs.open(load_path, mode="r") as fs_file:
                query = fs_file.read()
        elif "sql" in load_args:
            query = load_args.pop("sql")
        else:
            query = f"SELECT * FROM {self.table_name}"  # nosec B608

        return pl.read_database(
            query=query,
            connection=self.engine,
            **load_args
        )

    def save(self, data: pl.DataFrame) -> NoReturn:
        if not self.table_name:
            raise DatasetError(
                "'table_name' argument is required to save datasets."
            )

        data.write_database(
            table_name=self.table_name,
            connection=self._connection_str,
            **self._save_args
        )
