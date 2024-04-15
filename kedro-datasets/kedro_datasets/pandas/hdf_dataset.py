"""``HDFDataset`` loads/saves data from/to a hdf file using an underlying
filesystem (e.g.: local, S3, GCS). It uses pandas.HDFStore to handle the hdf file.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from threading import Lock
from typing import Any

import fsspec
import pandas as pd
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

HDFSTORE_DRIVER = "H5FD_CORE"


class HDFDataset(AbstractVersionedDataset[pd.DataFrame, pd.DataFrame]):
    """``HDFDataset`` loads/saves data from/to a hdf file using an underlying
    filesystem (e.g. local, S3, GCS). It uses pandas.HDFStore to handle the hdf file.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        hdf_dataset:
          type: pandas.HDFDataset
          filepath: s3://my_bucket/raw/sensor_reading.h5
          credentials: aws_s3_creds
          key: data

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.pandas import HDFDataset
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = HDFDataset(filepath=tmp_path / "test.h5", key="data")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.equals(reloaded)

    """

    # _lock is a class attribute that will be shared across all the instances.
    # It is used to make dataset safe for threads.
    _lock = Lock()
    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        key: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``HDFDataset`` pointing to a concrete hdf file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a hdf file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            key: Identifier to the group in the HDF store.
            load_args: PyTables options for loading hdf files.
                You can find all available arguments at:
                https://www.pytables.org/usersguide/libref/top_level.html#tables.open_file
                All defaults are preserved.
            save_args: PyTables options for saving hdf files.
                You can find all available arguments at:
                https://www.pytables.org/usersguide/libref/top_level.html#tables.open_file
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``), as well as
                to pass to the filesystem's `open` method through nested keys
                `open_args_load` and `open_args_save`.
                Here you can find all available arguments for `open`:
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.open
                All defaults are preserved, except `mode`, which is set `wb` when saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        self._key = key

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        _fs_open_args_save.setdefault("mode", "wb")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "key": self._key,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> pd.DataFrame:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            binary_data = fs_file.read()

        with HDFDataset._lock:
            # Set driver_core_backing_store to False to disable saving
            # contents of the in-memory h5file to disk
            with pd.HDFStore(
                "in-memory-load-file",
                mode="r",
                driver=HDFSTORE_DRIVER,
                driver_core_backing_store=0,
                driver_core_image=binary_data,
                **self._load_args,
            ) as store:
                return store[self._key]

    def _save(self, data: pd.DataFrame) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with HDFDataset._lock:
            with pd.HDFStore(
                "in-memory-save-file",
                mode="w",
                driver=HDFSTORE_DRIVER,
                driver_core_backing_store=0,
                **self._save_args,
            ) as store:
                store.put(self._key, data, format="table")
                binary_data = store._handle.get_file_image()

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            fs_file.write(binary_data)

        self._invalidate_cache()

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
