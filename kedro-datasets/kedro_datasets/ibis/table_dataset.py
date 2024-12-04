"""Provide data loading and saving functionality for Ibis's backends."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any, ClassVar

import ibis.expr.types as ir
from kedro.io import AbstractDataset

from kedro_datasets._utils import ConnectionMixin

if TYPE_CHECKING:
    from ibis import BaseBackend


class TableDataset(ConnectionMixin, AbstractDataset[ir.Table, ir.Table]):
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

    DEFAULT_CONNECTION_CONFIG: ClassVar[dict[str, Any]] = {
        "backend": "duckdb",
        "database": ":memory:",
    }
    DEFAULT_SAVE_ARGS: ClassVar[dict[str, Any]] = {
        "materialized": "view",
        "overwrite": True,
    }

    _CONNECTION_GROUP: ClassVar[str] = "ibis"

    def __init__(  # noqa: PLR0913
        self,
        *,
        table_name: str,
        connection: dict[str, Any] | None = None,
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
            connection: Configuration for connecting to an Ibis backend.
                If not provided, connect to DuckDB in in-memory mode.
            save_args: Additional arguments passed to the Ibis backend's
                `create_{materialized}` method. By default, ``ir.Table``
                objects are materialized as views. To save a table using
                a different materialization strategy, supply a value for
                `materialized` in `save_args`.
            metadata: Any arbitrary metadata. This is ignored by Kedro,
                but may be consumed by users or external plugins.
        """

        self._table_name = table_name
        self._connection_config = connection or self.DEFAULT_CONNECTION_CONFIG
        self.metadata = metadata

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        self._materialized = self._save_args.pop("materialized")

    def _connect(self) -> BaseBackend:
        import ibis

        config = deepcopy(self._connection_config)
        backend = getattr(ibis, config.pop("backend"))
        return backend.connect(**config)

    @property
    def connection(self) -> BaseBackend:
        """The ``Backend`` instance for the connection configuration."""
        return self._connection

    def load(self) -> ir.Table:
        return self.connection.table(self._table_name)

    def save(self, data: ir.Table) -> None:
        writer = getattr(self.connection, f"create_{self._materialized}")
        writer(self._table_name, data, **self._save_args)

    def _describe(self) -> dict[str, Any]:
        return {
            "table_name": self._table_name,
            "backend": self._connection_config["backend"],
            "save_args": self._save_args,
            "materialized": self._materialized,
        }

    def _exists(self) -> bool:
        return (
            self._table_name is not None and self._table_name in self.connection.tables
        )
