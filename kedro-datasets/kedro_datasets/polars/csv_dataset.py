"""``CSVDataset`` loads/saves data from/to a CSV file using an underlying
filesystem (e.g.: local, S3, GCS). It uses polars to handle the CSV file.
"""
from __future__ import annotations

import logging
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import polars as pl
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

logger = logging.getLogger(__name__)


class CSVDataset(AbstractVersionedDataset[pl.DataFrame, pl.DataFrame]):
    """``CSVDataset`` loads/saves data from/to a CSV file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses polars to handle the CSV file.

    Example usage for the `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: polars.CSVDataset
          filepath: data/01_raw/company/cars.csv
          load_args:
            sep: ","
            parse_dates: False
          save_args:
            has_header: False
            null_value: "somenullstring"

        motorbikes:
          type: polars.CSVDataset
          filepath: s3://your_bucket/data/02_intermediate/company/motorbikes.csv
          credentials: dev_s3

    Example usage for the
    `Python API <https://docs.kedro.org/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.polars import CSVDataset
        >>> import polars as pl
        >>>
        >>> data = pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})
        >>>
        >>> dataset = CSVDataset(filepath=tmp_path / "test.csv")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert data.frame_equal(reloaded)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {"rechunk": True}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}
    DEFAULT_FS_ARGS: dict[str, Any] = {"open_args_save": {"mode": "w"}}

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
        """Creates a new instance of ``CSVDataset`` pointing to a concrete CSV file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a CSV file prefixed with a protocol
                `s3://`.
                If prefix is not provided, `file` protocol (local filesystem)
                will be used.
                The prefix should be any protocol supported by ``fsspec``.
                Note: `http(s)` doesn't support versioning.
            load_args: Polars options for loading CSV files.
                Here you can find all available arguments:
                https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.read_csv.html#polars.read_csv
                All defaults are preserved, but we explicitly use `rechunk=True` for `seaborn`
                compatibility.
            save_args: Polars options for saving CSV files.
                Here you can find all available arguments:
                https://pola-rs.github.io/polars/py-polars/html/reference/api/polars.DataFrame.write_csv.html
                All defaults are preserved.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
                Defaults are preserved, apart from the `open_args_save` `mode` which is set to `w`.

            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        _fs_args = deepcopy(self.DEFAULT_FS_ARGS)
        if fs_args is not None:
            _fs_args.update(fs_args)

        self._fs_open_args_load = _fs_args.pop("open_args_load", {})
        self._fs_open_args_save = _fs_args.pop("open_args_save", {})
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

        # Handle default load and save arguments
        self._load_args = {**self.DEFAULT_LOAD_ARGS, **(load_args or {})}
        self._save_args = {**self.DEFAULT_SAVE_ARGS, **(save_args or {})}

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
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> pl.DataFrame:
        load_path = str(self._get_load_path())
        if self._protocol == "file":
            # file:// protocol seems to misbehave on Windows
            # (<urlopen error file not on local host>),
            # so we don't join that back to the filepath;
            # storage_options also don't work with local paths
            return pl.read_csv(load_path, **self._load_args)

        load_path = f"{self._protocol}{PROTOCOL_DELIMITER}{load_path}"
        return pl.read_csv(
            load_path, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pl.DataFrame) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            data.write_csv(file=fs_file, **self._save_args)

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
