"""``MatlabDataset`` loads/saves data from/to a Matlab file using an underlying
filesystem ?(e.g.: local, S3, GCS)?. The underlying functionality is supported by
the specified backend library passed in (defaults to the ``matlab`` library), so it
supports all allowed options for loading and saving matlab files.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import numpy as np
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)
from scipy import io


class MatlabDataset(AbstractVersionedDataset[np.ndarray, np.ndarray]):
    """`MatlabDataSet` loads and saves data from/to a MATLAB file using scipy.io.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: matlab.MatlabDataset
          filepath: gcs://your_bucket/cars.mat
          fs_args:
            project: my-project
          credentials: my_gcp_credentials

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.matlab import MatlabDataset
        >>> import numpy as np
        >>> data = np.array([1, 2, 3])
        >>> dataset = MatlabDataset(filepath=tmp_path / "test.mat")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert (data == reloaded["data"]).all()

    """

    DEFAULT_SAVE_ARGS: dict[str, Any] = {"indent": 2}

    def __init__(  # noqa = PLR0913
        self,
        filepath: str,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of MatlabDataSet to load and save data from/to a MATLAB file.

        Args:
            filepath: Filepath in POSIX format to a Matlab file prefixed with a protocol like `s3://`.
                If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            save_args: .mat options for saving .mat files.
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
                All defaults are preserved, except `mode`, which is set to `r` when loading
                and to `w` when saving.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _fs_open_args_load = _fs_args.pop("open_args_load", {})
        _fs_open_args_save = _fs_args.pop("open_args_save", {})
        _credentials = deepcopy(credentials) or {}

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
        # Handle default save arguments
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        _fs_open_args_save.setdefault("mode", "w")
        self._fs_open_args_load = _fs_open_args_load
        self._fs_open_args_save = _fs_open_args_save

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> np.ndarray:
        """
        Access the specific variable in the .mat file, e.g, data['variable_name']
        """
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        with self._fs.open(load_path) as f:
            data = io.loadmat(f)
            return data

    def _save(self, data: np.ndarray) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        with self._fs.open(save_path, mode="wb") as f:
            io.savemat(f, {"data": data})
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
