"""GenericDataset loads and saves data to a local file. The
underlying functionality is supported by geopandas, so it supports all
allowed geopandas (pandas) options for loading and saving geosjon files.
"""

from __future__ import annotations

import copy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import geopandas as gpd
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

# pyogrio currently supports no alternate file handlers https://github.com/geopandas/pyogrio/issues/430
gpd.options.io_engine = "fiona"

NON_FILE_SYSTEM_TARGETS = ["postgis"]


class GenericDataset(
    AbstractVersionedDataset[
        gpd.GeoDataFrame, gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame]
    ]
):
    """``GenericDataset`` loads/saves data to a file using an underlying filesystem
    (eg: local, S3, GCS).
    The underlying functionality is supported by geopandas, so it supports all
    allowed geopandas (pandas) options for loading and saving files.

    Examples:
        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> import geopandas as gpd
        >>> from kedro_datasets.geopandas import GenericDataset
        >>> from shapely.geometry import Point
        >>>
        >>> data = gpd.GeoDataFrame(
        ...     {"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]},
        ...     geometry=[Point(1, 1), Point(2, 4)],
        ... )
        >>>
        >>> dataset = GenericDataset(filepath=tmp_path / "test.geojson")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {"open_args_save": {"mode": "wb"}}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        file_format: str = "file",
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``GenericDataset`` pointing to a concrete file
        on a specific filesystem fsspec.

        Args:
            filepath: Filepath in POSIX format to a file prefixed with a protocol like
                `s3://`. If prefix is not provided `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            file_format: String which is used to match the appropriate load/save method on a best
                effort basis. For example if 'parquet' is passed in the `geopandas.read_parquet` and
                `geopandas.DataFrame.to_parquet` will be identified. An error will be raised unless
                at least one matching `read_{file_format}` or `to_{file_format}` method is
                identified. Defaults to 'file'.
            load_args: GeoPandas options for loading files.
                Here you can find all available arguments:
                https://geopandas.org/en/stable/docs/reference/api/geopandas.read_file.html
            save_args: GeoPandas options for saving files.
                Here you can find all available arguments:
                https://geopandas.org/en/stable/docs/reference/api/geopandas.GeoDataFrame.to_file.html
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
            credentials: credentials required to access the underlying filesystem.
                Eg. for ``GCFileSystem`` it would look like `{'token': None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set to `wb` when saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """

        self._file_format = file_format.lower()

        _fs_args = copy.deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = copy.deepcopy(credentials) or {}
        protocol, path = get_protocol_and_path(filepath, version)
        self._protocol = protocol
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load and save and fs arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}
        self._fs_open_args_load = {
            **self.DEFAULT_FS_ARGS.get("open_args_load", {}),
            **(_fs_open_args_load or {}),
        }
        self._fs_open_args_save = {
            **self.DEFAULT_FS_ARGS.get("open_args_save", {}),
            **(_fs_open_args_save or {}),
        }

    def _ensure_file_system_target(self) -> None:
        # Fail fast if provided a known non-filesystem target
        if self._file_format in NON_FILE_SYSTEM_TARGETS:
            raise DatasetError(
                f"Cannot load or save a dataset of file_format '{self._file_format}' as it "
                f"does not support a filepath target/source."
            )

    def load(self) -> gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame]:
        self._ensure_file_system_target()

        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        load_method = getattr(gpd, f"read_{self._file_format}", None)
        if load_method:
            with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
                return load_method(fs_file, **self._load_args)
        raise DatasetError(
            f"Unable to retrieve 'geopandas.read_{self._file_format}' method, please ensure that your "
            "'file_format' parameter has been defined correctly as per the GeoPandas API "
            "https://geopandas.org/en/stable/docs/reference/io.html"
        )

    def save(self, data: gpd.GeoDataFrame) -> None:
        self._ensure_file_system_target()

        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        save_method = getattr(data, f"to_{self._file_format}", None)
        if save_method:
            with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
                # KEY ASSUMPTION - first argument is path/buffer/io
                save_method(fs_file, **self._save_args)
                self.invalidate_cache()
        else:
            raise DatasetError(
                f"Unable to retrieve 'geopandas.DataFrame.to_{self._file_format}' method, please "
                "ensure that your 'file_format' parameter has been defined correctly as "
                "per the GeoPandas API "
                "https://geopandas.org/en/stable/docs/reference/io.html"
            )

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False
        return self._fs.exists(load_path)

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "file_format": self._file_format,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _release(self) -> None:
        self.invalidate_cache()

    def invalidate_cache(self) -> None:
        """Invalidate underlying filesystem cache."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
