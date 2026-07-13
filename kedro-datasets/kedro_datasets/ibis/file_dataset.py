"""Provide file loading and saving functionality for Ibis's backends."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, ClassVar

import fsspec
import ibis.expr.types as ir
from kedro.io import AbstractVersionedDataset, DatasetError, Version

from kedro_datasets._utils import ConnectionMixin
from kedro_datasets._utils.file_utils import split_filepath

if TYPE_CHECKING:
    from ibis import BaseBackend


class FileDataset(ConnectionMixin, AbstractVersionedDataset[ir.Table, ir.Table]):
    """``FileDataset`` loads/saves data from/to a specified file format.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        cars:
          type: ibis.FileDataset
          filepath: data/01_raw/company/cars.csv
          file_format: csv
          table_name: cars
          connection:
            backend: duckdb
            database: company.db
          load_args:
            sep: ","
            nullstr: "#NA"
          save_args:
            sep: ","
            nullstr: "#NA"

        motorbikes:
          type: ibis.FileDataset
          filepath: s3://your_bucket/data/02_intermediate/company/motorbikes/
          file_format: delta
          table_name: motorbikes
          connection:
            backend: polars
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> import ibis
        >>> from kedro_datasets.ibis import FileDataset
        >>>
        >>> data = ibis.memtable({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = FileDataset(
        ...     filepath=tmp_path / "test.csv",
        ...     file_format="csv",
        ...     table_name="test",
        ...     connection={"backend": "duckdb", "database": tmp_path / "file.db"},
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
    DEFAULT_SAVE_ARGS: ClassVar[dict[str, Any]] = {}

    _CONNECTION_GROUP: ClassVar[str] = "ibis"

    def __init__(  # noqa: PLR0913
        self,
        filepath: str,
        file_format: str = "parquet",
        *,
        table_name: str | None = None,
        connection: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        version: Version | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new ``FileDataset`` pointing to the given filepath.

        ``FileDataset`` connects to the Ibis backend object constructed
        from the connection configuration. The `backend` key provided in
        the config can be any of the
        [supported backends](https://ibis-project.org/install). The
        remaining dictionary entries will be passed as arguments to the
        underlying ``connect()`` method (e.g.
        [ibis.duckdb.connect()](https://ibis-project.org/backends/duckdb#ibis.duckdb.connect)).

        The read method corresponding to the given ``file_format`` (e.g.
        [read_csv()](https://ibis-project.org/backends/duckdb#ibis.backends.duckdb.Backend.read_csv))
        is used to load
        the file with the backend. Note that only the data is loaded; no
        link to the underlying file exists past ``FileDataset.load()``.

        Args:
            filepath: Path to a file to register as a table. Most useful
                for loading data into your data warehouse (for testing).
                On save, the backend exports data to the specified path.
            file_format: String specifying the file format for the file.
                Defaults to writing execution results to a Parquet file.
            table_name: The name to use for the created table (on load).
            connection: Configuration for connecting to an Ibis backend.
                If not provided, connect to DuckDB in in-memory mode.
            credentials: Credentials or additional configuration used to
                connect (e.g. user, password, token, account). If given,
                these values override the base connection configuration.
            load_args: Additional arguments passed to the Ibis backend's
                `read_{file_format}` method.
            save_args: Additional arguments passed to the Ibis backend's
                `to_{file_format}` method.
            fs_args: Extra arguments to pass into underlying filesystem class
                constructor (e.g. ``{"project": "my-project"}`` for
                ``GCSFileSystem``). Used only to discover versions and check
                existence of a remote ``filepath``; reading and writing is left
                to the Ibis backend. Note that fsspec credentials go here, not
                in ``credentials`` (which configures the Ibis connection).
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            metadata: Any arbitrary metadata. This is ignored by Kedro,
                but may be consumed by users or external plugins.
        """
        self._file_format = file_format
        self._table_name = table_name
        _connection_config = connection or self.DEFAULT_CONNECTION_CONFIG
        _credentials = deepcopy(credentials) or {}
        self._connection_config = {**_connection_config, **_credentials}
        self._fs_args = deepcopy(fs_args or {})
        self.metadata = metadata

        self._fs_prefix, path = split_filepath(filepath)
        self._fs: fsspec.AbstractFileSystem | None = None

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs_exists if self._fs_prefix else None,
            glob_function=self._fs_glob if self._fs_prefix else None,
        )

        # Set load and save arguments, overwriting defaults if provided.
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)

        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _connect(self) -> BaseBackend:
        import ibis  # noqa: PLC0415

        config = deepcopy(self._connection_config)
        backend = getattr(ibis, config.pop("backend"))
        return backend.connect(**config)

    @property
    def connection(self) -> BaseBackend:
        """The ``Backend`` instance for the connection configuration."""
        return self._connection

    def _filesystem(self) -> fsspec.AbstractFileSystem:
        # Build lazily so backend-only users don't need adlfs, s3fs, etc.
        if self._fs is None:
            protocol = self._fs_prefix.removesuffix("://")
            self._fs = fsspec.filesystem(protocol, **self._fs_args)
        return self._fs

    def _fs_exists(self, path: str) -> bool:
        return self._filesystem().exists(path)

    def _fs_glob(self, pattern: str) -> list[str]:
        return self._filesystem().glob(pattern)

    def load(self) -> ir.Table:
        load_path = self._fs_prefix + str(self._get_load_path())
        reader = getattr(self.connection, f"read_{self._file_format}")
        return reader(load_path, table_name=self._table_name, **self._load_args)

    def save(self, data: ir.Table) -> None:
        save_path = self._fs_prefix + str(self._get_save_path())
        if not self._fs_prefix:  # only local paths need their parent created
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        writer = getattr(self.connection, f"to_{self._file_format}")
        writer(data, save_path, **self._save_args)

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._fs_prefix + str(self._filepath),
            "file_format": self._file_format,
            "table_name": self._table_name,
            "backend": self._connection_config["backend"],
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _exists(self) -> bool:
        try:
            load_path = self._get_load_path()
        except DatasetError:
            return False

        return self._exists_function(str(load_path))
