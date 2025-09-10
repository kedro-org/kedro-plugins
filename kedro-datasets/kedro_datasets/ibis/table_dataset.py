"""Provide data loading and saving functionality for Ibis's backends."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar

import ibis.expr.types as ir
import pandas as pd
from kedro.io import AbstractDataset, DatasetError

from kedro_datasets._utils import ConnectionMixin

if TYPE_CHECKING:
    from ibis import BaseBackend


class TableDataset(ConnectionMixin, AbstractDataset[ir.Table, ir.Table]):
    """`TableDataset` loads/saves data from/to Ibis table expressions.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        cars:
          type: ibis.TableDataset
          table_name: cars
          connection:
            backend: duckdb
            database: company.db
          save_args:
            materialized: table
            mode: append

        motorbikes:
          type: ibis.TableDataset
          table_name: motorbikes
          connection:
            backend: duckdb
            database: company.db
          save_args:
            materialized: view
            mode: overwrite
    ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import ibis
        >>> from kedro_datasets.ibis import TableDataset
        >>>
        >>> data = ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = TableDataset(
        ...     table_name="test",
        ...     connection={"backend": "duckdb", "database": tmp_path / "file.db"},
        ...     save_args={"materialized": "table"},
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.execute().equals(reloaded.execute())

    """

    DEFAULT_CONNECTION_CONFIG: ClassVar[dict[str, Any]] = {
        "backend": "duckdb",
        "database": ":memory:",
    }
    DEFAULT_LOAD_ARGS: ClassVar[dict[str, Any]] = {}
    DEFAULT_SAVE_ARGS: ClassVar[dict[str, Any]] = {
        "materialized": "view",
        "mode": "overwrite",
    }
    # Supported modes for saving behavior.
    _ALLOWED_MODES: ClassVar[set[str]] = {
        "append",
        "overwrite",
        "error",
        "errorifexists",
        "ignore",
    }

    _CONNECTION_GROUP: ClassVar[str] = "ibis"

    def __init__(  # noqa: PLR0913
        self,
        *,
        table_name: str,
        database: str | None = None,
        credentials: dict[str, Any] | str | None = None,
        connection: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``TableDataset`` pointing to a table.

        ``TableDataset`` connects to the Ibis backend object constructed
        from the connection configuration. The `backend` key provided in
        the config can be any of the `supported backends <https://ibis-\
        project.org/install>`_. The remaining dictionary entries will be
        passed as arguments to the underlying ``connect()`` method (e.g.
        `ibis.duckdb.connect() <https://ibis-project.org/backends/duckdb\
        #ibis.duckdb.connect>`_).

        The dataset establishes a connection to the relevant table for the execution
        backend. Therefore, Ibis doesn't fetch data on load; all compute
        is deferred until materialization, when the expression is saved.
        In practice, this happens when another ``TableDataset`` instance
        is saved, after running code defined across one more more nodes.

        Args:
            table_name: The name of the table or view to read or create.
            database: The name of the database to read the table or view
                from or create the table or view in. If not passed, then
                the current database is used. Provide a tuple of strings
                (e.g. `("catalog", "database")`) or a dotted string path
                (e.g. `"catalog.database"`) to reference a table or view
                in a multi-level table hierarchy.
            connection: Configuration for connecting to an Ibis backend.
                If not provided, connect to DuckDB in in-memory mode.
            credentials: Connection information (e.g.
                user, password, token, account). If provided, these values
                overlay the base `connection` configuration. May also be a
                connection string.
            load_args: Additional arguments passed to the Ibis backend's
                `read_{file_format}` method.
            save_args: Additional arguments passed to the Ibis backend's
                `create_{materialized}` method. By default, ``ir.Table``
                objects are materialized as views. To save a table using
                a different materialization strategy, supply a value for
                `materialized` in `save_args`. The `mode` parameter controls
                the behavior when saving data:
                - _"overwrite"_: Overwrite existing data in the table.
                - _"append"_: Append contents of the new data to the existing table (does not overwrite).
                - _"error"_ or _"errorifexists"_: Throw an exception if the table already exists.
                - _"ignore"_: Silently ignore the operation if the table already exists.
            metadata: Any arbitrary metadata. This is ignored by Kedro,
                but may be consumed by users or external plugins.
        """

        self._table_name = table_name
        self._database = database
        self._credentials = deepcopy(credentials) or {}
        if self._credentials != {}:
            self._backend_name = self._get_backend_name()

        # Prefer credentials if provided, else fallback to connection.
        self._connection_config = connection or (
            self.DEFAULT_CONNECTION_CONFIG if self._credentials == {} else {}
        )
        if self._credentials is not None:
            if isinstance(self._credentials, str):
                self._connection_config.update(
                    {
                        "backend": self._backend_name,
                        "con": self._credentials,
                    }
                )
            elif (
                isinstance(self._credentials, dict)
                and "backend" not in self._credentials
                and "con" in self._credentials
            ):
                self._connection_config.update(
                    self._credentials | {"backend": self._backend_name}
                )
            else:
                self._connection_config.update(self._credentials)
        self.metadata = metadata

        # Set load and save arguments, overwriting defaults if provided.
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        if database is not None:
            self._load_args["database"] = database

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)
        if database is not None:
            self._save_args["database"] = database

        self._materialized = self._save_args.pop("materialized")

        # Handle mode / overwrite conflict
        if save_args and "mode" in save_args and "overwrite" in self._save_args:
            raise ValueError("Cannot specify both 'mode' and deprecated 'overwrite'.")
        # Map legacy overwrite if present
        if "overwrite" in self._save_args:
            legacy = self._save_args.pop("overwrite")
            # remove any lingering 'mode' key from defaults to avoid leaking into writer kwargs
            self._save_args.pop("mode", None)
            self._mode = "overwrite" if legacy else "error"
        else:
            self._mode = self._save_args.pop("mode")

        if self._mode not in self._ALLOWED_MODES:
            raise ValueError(f"Invalid 'mode' value: {self._mode}.")

    def _connect(self) -> BaseBackend:
        import ibis  # noqa: PLC0415

        config = deepcopy(self._connection_config)
        # If credentials is a dict with a 'con' key, treat as connection string
        if isinstance(config, dict) and "con" in config:
            return ibis.connect(config.pop("con"))
        # Otherwise, treat as expanded dict (params)
        backend = getattr(ibis, config.pop("backend"))
        return backend.connect(**config)

    @property
    def connection(self) -> BaseBackend:
        """The ``Backend`` instance for the connection configuration."""
        return self._connection

    def load(self) -> ir.Table:
        return self.connection.table(self._table_name, **self._load_args)

    # Users should wrap their DataFrame-like object in an ibis.memtable for in-memory data.
    # https://github.com/ibis-project/ibis/blob/df4f1858e7f9bc36d589dcf2a53f9f16b1acd92a/ibis/backends/duckdb/__init__.py#L180
    def save(self, data: ir.Table) -> None:
        # treat empty ir.Table as a no-op
        if isinstance(data, ir.Table) and data.count().execute() == 0:
            return
        writer = getattr(self.connection, f"create_{self._materialized}")
        if self._mode == "append":
            if not self._exists():
                writer(self._table_name, data, overwrite=False, **self._save_args)
            elif hasattr(self.connection, "insert"):
                self.connection.insert(self._table_name, data, **self._save_args)
            else:
                raise DatasetError(
                    f"The {self.connection.name} backend for Ibis does not support inserts."
                )
            return

        if self._mode == "overwrite":
            writer(self._table_name, data, overwrite=True, **self._save_args)
        elif self._mode in {"error", "errorifexists"}:
            writer(self._table_name, data, overwrite=False, **self._save_args)
        elif self._mode == "ignore":
            if self._exists():
                return
            writer(self._table_name, data, overwrite=False, **self._save_args)

    def _get_backend_name(self) -> str | None:
        """Get the backend name from the connection config or connection string."""
        config = self._credentials
        # If dict and has 'backend'
        if isinstance(config, dict) and "backend" in config:
            return config["backend"]
        # If string, parse as backend://...
        if isinstance(config, str):
            if "://" in config:
                return config.split("://", 1)[0]
        # If dict with 'con' key
        if isinstance(config, dict) and "con" in config:
            con_str = config["con"]
            if "://" in con_str:
                return con_str.split("://", 1)[0]

    def _describe(self) -> dict[str, Any]:
        load_args = deepcopy(self._load_args)
        save_args = deepcopy(self._save_args)
        load_args.pop("database", None)
        save_args.pop("database", None)
        return {
            "table_name": self._table_name,
            "database": self._database,
            "backend": self.connection.name,
            "load_args": load_args,
            "save_args": save_args,
            "materialized": self._materialized,
            "mode": self._mode,
        }

    def _exists(self) -> bool:
        return (
            self._table_name is not None and self._table_name in self.connection.tables
        )
