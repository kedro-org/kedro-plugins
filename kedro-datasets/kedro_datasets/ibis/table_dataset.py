"""Provide data loading and saving functionality for Ibis's backends."""
from __future__ import annotations

import warnings
from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar

import ibis.expr.types as ir
from kedro.io import AbstractDataset, DatasetError

from kedro_datasets import KedroDeprecationWarning

if TYPE_CHECKING:
    from ibis import BaseBackend


class TableDataset(AbstractDataset[ir.Table, ir.Table]):
    """``TableDataset`` loads/saves data from/to Ibis table expressions.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: ibis.TableDataset
          table_name: cars
          connection:
            backend: duckdb
            database: company.db
          save_args:
            materialized: table

        motorbikes:
          type: ibis.TableDataset
          table_name: motorbikes
          connection:
            backend: duckdb
            database: company.db

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

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

    DEFAULT_LOAD_ARGS: ClassVar[dict[str, Any]] = {}
    DEFAULT_SAVE_ARGS: ClassVar[dict[str, Any]] = {
        "materialized": "view",
        "overwrite": True,
    }

    _connections: ClassVar[dict[tuple[tuple[str, str]], BaseBackend]] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str | None = None,
        file_format: str | None = None,
        table_name: str | None = None,
        connection: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``TableDataset`` pointing to a table (or file).

        ``TableDataset`` connects to the Ibis backend object constructed
        from the connection configuration. The `backend` key provided in
        the config can be any of the `supported backends <https://ibis-\
        project.org/install>`_. The remaining dictionary entries will be
        passed as arguments to the underlying ``connect()`` method (e.g.
        `ibis.duckdb.connect() <https://ibis-project.org/backends/duckdb\
        #ibis.duckdb.connect>`_).

        If ``table_name`` is given (and ``filepath`` isn't), the dataset
        establishes a connection to the relevant table for the execution
        backend. Therefore, Ibis doesn't fetch data on load; all compute
        is deferred until materialization, when the expression is saved.
        In practice, this happens when another ``TableDataset`` instance
        is saved, after running code defined across one more more nodes.

        Args:
            table_name: The name of the table or view to read or create.
            connection: Configuration for connecting to an Ibis backend.
            load_args: Additional arguments passed to the Ibis backend's
                `read_{file_format}` method.
            save_args: Additional arguments passed to the Ibis backend's
                `create_{materialized}` method. By default, ``ir.Table``
                objects are materialized as views. To save a table using
                a different materialization strategy, supply a value for
                `materialized` in `save_args`.
            metadata: Any arbitrary metadata. This is ignored by Kedro,
                but may be consumed by users or external plugins.
        """
        if filepath is None and table_name is None:
            raise DatasetError(
                "Must provide at least one of `filepath` or `table_name`."
            )

        if filepath is not None or file_format is not None:
            warnings.warn(
                "Use 'FileDataset' to load and save files with an Ibis "
                "backend; the functionality will be removed from 'Table"
                "Dataset' in Kedro-Datasets 6.0.0",
                KedroDeprecationWarning,
                stacklevel=2,
            )

        self._filepath = filepath
        self._file_format = file_format
        self._table_name = table_name
        self._connection_config = connection
        self.metadata = metadata

        # Set load and save arguments, overwriting defaults if provided.
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._materialized = self._save_args.pop("materialized")

    @property
    def connection(self) -> BaseBackend:
        def hashable(value):
            if isinstance(value, dict):
                return tuple((k, hashable(v)) for k, v in sorted(value.items()))
            if isinstance(value, list):
                return tuple(hashable(x) for x in value)
            return value

        cls = type(self)
        key = hashable(self._connection_config)
        if key not in cls._connections:
            import ibis

            config = deepcopy(self._connection_config)
            backend_attr = config.pop("backend") if config else None
            backend = getattr(ibis, backend_attr)
            cls._connections[key] = backend.connect(**config)

        return cls._connections[key]

    def load(self) -> ir.Table:
        if self._filepath is not None:
            if self._file_format is None:
                raise NotImplementedError

            reader = getattr(self.connection, f"read_{self._file_format}")
            return reader(self._filepath, self._table_name, **self._load_args)
        else:
            return self.connection.table(self._table_name)

    def save(self, data: ir.Table) -> None:
        if self._table_name is None:
            raise DatasetError("Must provide `table_name` for materialization.")

        writer = getattr(self.connection, f"create_{self._materialized}")
        writer(self._table_name, data, **self._save_args)

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "file_format": self._file_format,
            "table_name": self._table_name,
            "backend": self._connection_config.get("backend")
            if self._connection_config
            else None,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "materialized": self._materialized,
        }

    def _exists(self) -> bool:
        return (
            self._table_name is not None and self._table_name in self.connection.tables
        )
