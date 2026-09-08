"""``IcebergDataset`` loads and saves data from/to Apache Iceberg tables.

Loads via Polars ``scan_iceberg`` when available, with a PyIceberg scan fallback.
Saves data to Iceberg tables via PyIceberg catalog operations. Catalog access is handled through PyIceberg.
"""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from kedro.io.core import AbstractDataset, DatasetError

if TYPE_CHECKING:
    import polars as pl


class IcebergDataset(AbstractDataset):
    """``IcebergDataset`` loads and saves data from/to Apache Iceberg tables.

    Loads via Polars ``scan_iceberg`` when available, with a PyIceberg scan fallback.
    Saves data to Iceberg tables via PyIceberg catalog operations. Catalog access is handled through PyIceberg.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        sales_iceberg_polars:
          type: polars.IcebergDataset
          table_name: analytics.sales
          catalog_name: glue_catalog
          catalog_properties:
            type: glue
          credentials: glue_credentials
          load_args:
            snapshot_id: 1234567890
          save_args:
            mode: overwrite
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from kedro_datasets.polars import IcebergDataset  # doctest: +SKIP
        >>> import polars as pl  # doctest: +SKIP
        >>>
        >>> data = pl.DataFrame({"col1": [1, 2], "col2": [4, 5]})  # doctest: +SKIP
        >>> dataset = IcebergDataset(  # doctest: +SKIP
        ...     table_name="default.my_table",
        ...     catalog_properties={"type": "sql", "uri": "sqlite:///test.db"}
        ... )
        >>> dataset.save(data)  # doctest: +SKIP
        >>> reloaded = dataset.load()  # doctest: +SKIP
        >>> assert data.equals(reloaded)  # doctest: +SKIP

    """

    DEFAULT_WRITE_MODE = "overwrite"
    ACCEPTED_WRITE_MODES = ("overwrite", "append")

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"mode": DEFAULT_WRITE_MODE}

    def __init__(  # noqa: PLR0913
        self,
        *,
        table_name: str,
        catalog_name: str | None = None,
        catalog_properties: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``IcebergDataset`` pointing to an Apache Iceberg table.

        Args:
            table_name: Table identifier (e.g. ``"namespace.table_name"`` or ``"table_name"``).
            catalog_name: Name of the Iceberg catalog to load. Defaults to None.
            catalog_properties: Properties required to instantiate the catalog (e.g.
                ``{"type": "rest", "uri": "https://..."}`` or ``{"type": "glue"}``).
                Do not pass secrets/tokens here as they are visible in ``_describe()``;
                use ``credentials`` instead.
            credentials: Authentication credentials or secrets (e.g. tokens, AWS/GCP keys).
                These are merged with ``catalog_properties`` when connecting to the catalog
                and are safely excluded from ``_describe()`` to avoid leaking secrets.
            load_args: Additional scan options passed to ``polars.scan_iceberg``
                (e.g. ``snapshot_id``, ``storage_options``, ``reader_override``,
                ``use_metadata_statistics``, ``use_pyiceberg_filter``).
            save_args: Additional save options. Supported keys:
                - ``mode``: ``"overwrite"`` (default) or ``"append"``.
                - ``location``: Explicit storage location for newly created tables.
                - ``snapshot_properties``: Custom properties to attach to the snapshot summary.
            metadata: Any arbitrary metadata to attach to the dataset.

        Raises:
            DatasetError: If an invalid write mode or configuration is provided.
        """
        self._table_name = table_name
        self._catalog_name = catalog_name
        self._catalog_properties = deepcopy(catalog_properties or {})
        self._credentials = deepcopy(credentials or {})
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._metadata = metadata

        write_mode = self._save_args.get("mode", self.DEFAULT_WRITE_MODE)
        if write_mode not in self.ACCEPTED_WRITE_MODES:
            raise DatasetError(
                f"Write mode '{write_mode}' is not supported. "
                f"Please use one of {self.ACCEPTED_WRITE_MODES}."
            )

    def _get_catalog(self) -> Any:
        try:
            import pyiceberg.catalog as pyiceberg_catalog  # noqa: PLC0415
        except ImportError as exc:
            raise DatasetError(
                "PyIceberg is required to use 'polars.IcebergDataset'. "
                "Please install it using 'pip install pyiceberg'."
            ) from exc

        properties = {**self._catalog_properties, **self._credentials}
        return pyiceberg_catalog.load_catalog(self._catalog_name, **properties)

    def _load(self) -> pl.DataFrame:
        """Loads data from the Iceberg table into a Polars DataFrame."""
        try:
            import polars as pl  # noqa: PLC0415
        except ImportError as exc:
            raise DatasetError(
                "Polars is required to load data using 'polars.IcebergDataset'. "
                "Please install it using 'pip install polars'."
            ) from exc

        catalog = self._get_catalog()
        table = catalog.load_table(self._table_name)

        # Prefer native Polars Iceberg scan when available
        if hasattr(pl, "scan_iceberg"):
            try:
                lazy_df = pl.scan_iceberg(table, **self._load_args)
                return lazy_df.collect()
            except (NotImplementedError, AttributeError):
                # Fall back to PyIceberg scan if native scan is not supported for this table format/engine
                pass

        # Fallback: PyIceberg scan -> to_polars or Arrow zero-copy
        scan_args = {}
        if "snapshot_id" in self._load_args:
            scan_args["snapshot_id"] = self._load_args["snapshot_id"]

        scan = table.scan(**scan_args)
        if hasattr(scan, "to_polars"):
            return scan.to_polars()

        arrow_table = scan.to_arrow()
        return pl.from_arrow(arrow_table)

    def _save(self, data: pl.DataFrame | pl.LazyFrame) -> None:
        """Saves a Polars DataFrame into the Apache Iceberg table."""
        if hasattr(data, "collect"):
            data = data.collect()

        save_args = deepcopy(self._save_args)
        mode = save_args.pop("mode", self.DEFAULT_WRITE_MODE)
        location = save_args.pop("location", None)
        snapshot_properties = save_args.pop("snapshot_properties", None)

        catalog = self._get_catalog()
        arrow_table = data.to_arrow()

        # We write via PyIceberg's Arrow interface (append / overwrite) directly
        # rather than experimental/unstable polars write_iceberg/sink_iceberg,
        # ensuring atomic commits, snapshot isolation, partition spec compliance,
        # and schema enforcement across all catalog types (Glue, REST, SQL, etc.).
        if not self._exists():
            table = catalog.create_table(
                identifier=self._table_name,
                schema=arrow_table.schema,
                location=location,
                **save_args,
            )
            table.append(arrow_table, snapshot_properties=snapshot_properties or {})
        else:
            table = catalog.load_table(self._table_name)
            if mode == "append":
                table.append(arrow_table, snapshot_properties=snapshot_properties or {})
            else:
                table.overwrite(
                    arrow_table, snapshot_properties=snapshot_properties or {}
                )

    def _exists(self) -> bool:
        """Checks if the Iceberg table exists in the configured catalog."""
        _not_found_errors: tuple[type[Exception], ...] = ()
        try:
            from pyiceberg.exceptions import (  # noqa: PLC0415
                NoSuchNamespaceError,
                NoSuchTableError,
            )

            _not_found_errors = (NoSuchTableError, NoSuchNamespaceError)
        except ImportError:
            pass

        try:
            catalog = self._get_catalog()
            return catalog.table_exists(self._table_name)
        except _not_found_errors:
            return False

    def _describe(self) -> dict[str, Any]:
        return {
            "table_name": self._table_name,
            "catalog_name": self._catalog_name,
            "catalog_properties": self._catalog_properties,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }
