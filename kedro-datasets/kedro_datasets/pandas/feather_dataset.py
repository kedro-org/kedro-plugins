"""``FeatherDataset`` is a data set used to load and save data to feather files
using an underlying filesystem (e.g.: local, S3, GCS). The underlying functionality
is supported by pandas, so it supports all operations the pandas supports.
"""
from __future__ import annotations

import logging
from copy import deepcopy
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any

import fsspec
import pandas as pd
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataset,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

logger = logging.getLogger(__name__)


class FeatherDataset(AbstractVersionedDataset[pd.DataFrame, pd.DataFrame]):
    """``FeatherDataset`` loads and saves data to a feather file using an
    underlying filesystem (e.g.: local, S3, GCS). The underlying functionality
    is supported by pandas, so it supports all allowed pandas options
    for loading and saving csv files.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: pandas.FeatherDataset
          filepath: data/01_raw/company/cars.feather
          load_args:
            columns: ['col1', 'col2', 'col3']
            use_threads: True

        motorbikes:
          type: pandas.FeatherDataset
          filepath: s3://your_bucket/data/02_intermediate/company/motorbikes.feather
          credentials: dev_s3

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.pandas import FeatherDataset
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = FeatherDataset(filepath=tmp_path / "test.feather")
        >>>
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>>
        >>> assert data.equals(reloaded)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"mode": "wb"}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``FeatherDataset`` pointing to a concrete
        filepath.

        Args:
            filepath: Filepath in POSIX format to a feather file prefixed with a protocol like
                `s3://`. If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            load_args: Pandas options for loading feather files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_feather.html
                All defaults are preserved.
            save_args: Pandas options for saving feather files.
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_feather.html
                All defaults are preserved, apart from "mode", which is set to "wb".
                Note that the save method requires bytes, so any save mode provided should include "b" for bytes.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._storage_options = {**_credentials, **_fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        self.metadata = metadata

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load argument
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

        if "storage_options" in self._save_args or "storage_options" in self._load_args:
            logger.warning(
                "Dropping 'storage_options' for %s, "
                "please specify them under 'fs_args' or 'credentials'.",
                self._filepath,
            )
            self._save_args.pop("storage_options", None)
            self._load_args.pop("storage_options", None)

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "version": self._version,
        }

    def _load(self) -> pd.DataFrame:
        load_path = str(self._get_load_path())
        if self._protocol == "file":
            # file:// protocol seems to misbehave on Windows
            # (<urlopen error file not on local host>),
            # so we don't join that back to the filepath;
            # storage_options also don't work with local paths
            return pd.read_feather(load_path, **self._load_args)

        load_path = f"{self._protocol}{PROTOCOL_DELIMITER}{load_path}"
        return pd.read_feather(
            load_path, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pd.DataFrame) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        save_mode = self._save_args.get("mode")

        buf = BytesIO()
        data.to_feather(buf, **self._save_args)

        with self._fs.open(save_path, mode=save_mode) as fs_file:
            fs_file.write(buf.getvalue())

        self._invalidate_cache()

    def _exists(self) -> bool:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)
        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
