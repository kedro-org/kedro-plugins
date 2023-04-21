"""``ParquetDataSet`` loads/saves data from/to a Parquet file using an underlying
filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Parquet file.
"""
import logging
from copy import deepcopy
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import fsspec
import pandas as pd
import pyarrow.parquet as pq
from kedro.io.core import (
    PROTOCOL_DELIMITER,
    AbstractVersionedDataSet,
    DataSetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

logger = logging.getLogger(__name__)


class ParquetDataSet(AbstractVersionedDataSet[pd.DataFrame, pd.DataFrame]):
    """``ParquetDataSet`` loads/saves data from/to a Parquet file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses pandas to handle the Parquet file.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        boats:
          type: pandas.ParquetDataSet
          filepath: data/01_raw/boats.parquet
          load_args:
            engine: pyarrow
            use_nullable_dtypes: True
          save_args:
            file_scheme: hive
            has_nulls: False
            engine: pyarrow

        trucks:
          type: pandas.ParquetDataSet
          filepath: abfs://container/02_intermediate/trucks.parquet
          credentials: dev_abs
          load_args:
            columns: [name, gear, disp, wt]
            index: name
          save_args:
            compression: GZIP
            partition_on: [name]

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
    ::

        >>> from kedro_datasets.pandas import ParquetDataSet
        >>> import pandas as pd
        >>>
        >>> data = pd.DataFrame({'col1': [1, 2], 'col2': [4, 5],
        >>>                      'col3': [5, 6]})
        >>>
        >>> data_set = ParquetDataSet(filepath="test.parquet")
        >>> data_set.save(data)
        >>> reloaded = data_set.load()
        >>> assert data.equals(reloaded)

    """

    DEFAULT_LOAD_ARGS = {}  # type: Dict[str, Any]
    DEFAULT_SAVE_ARGS = {}  # type: Dict[str, Any]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
        credentials: Dict[str, Any] = None,
        fs_args: Dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``ParquetDataSet`` pointing to a concrete Parquet file
        on a specific filesystem.

        Args:
            filepath: Filepath in POSIX format to a Parquet file prefixed with a protocol like
                `s3://`. If prefix is not provided, `file` protocol (local filesystem) will be used.
                The prefix should be any protocol supported by ``fsspec``.
                It can also be a path to a directory. If the directory is
                provided then it can be used for reading partitioned parquet files.
                Note: `http(s)` doesn't support versioning.
            load_args: Additional options for loading Parquet file(s).
                Here you can find all available arguments when reading single file:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_parquet.html
                Here you can find all available arguments when reading partitioned datasets:
                https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html#pyarrow.parquet.ParquetDataset.read
                All defaults are preserved.
            save_args: Additional saving options for saving Parquet file(s).
                Here you can find all available arguments:
                https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_parquet.html
                All defaults are preserved. ``partition_cols`` is not supported.
            version: If specified, should be an instance of ``kedro.io.core.Version``.
                If its ``load`` attribute is None, the latest version will be loaded. If
                its ``save`` attribute is None, save version will be autogenerated.
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
        """
        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, path = get_protocol_and_path(filepath, version)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._protocol = protocol
        self._storage_options = {**_credentials, **_fs_args}
        self._fs = fsspec.filesystem(self._protocol, **self._storage_options)

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

        # Handle default load and save arguments
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

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> pd.DataFrame:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        if self._fs.isdir(load_path):
            # It doesn't work at least on S3 if root folder was created manually
            # https://issues.apache.org/jira/browse/ARROW-7867
            data = (
                pq.ParquetDataset(load_path, filesystem=self._fs)
                .read(**self._load_args)
                .to_pandas()
            )
        else:
            data = self._load_from_pandas()

        return data

    def _load_from_pandas(self):
        load_path = str(self._get_load_path())
        if self._protocol == "file":
            # file:// protocol seems to misbehave on Windows
            # (<urlopen error file not on local host>),
            # so we don't join that back to the filepath;
            # storage_options also don't work with local paths
            return pd.read_parquet(load_path, **self._load_args)

        load_path = f"{self._protocol}{PROTOCOL_DELIMITER}{load_path}"
        return pd.read_parquet(
            load_path, storage_options=self._storage_options, **self._load_args
        )

    def _save(self, data: pd.DataFrame) -> None:
        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        if Path(save_path).is_dir():
            raise DataSetError(
                f"Saving {self.__class__.__name__} to a directory is not supported."
            )

        if "partition_cols" in self._save_args:
            raise DataSetError(
                f"{self.__class__.__name__} does not support save argument "
                f"'partition_cols'. Please use 'kedro.io.PartitionedDataSet' instead."
            )

        bytes_buffer = BytesIO()
        data.to_parquet(bytes_buffer, **self._save_args)

        with self._fs.open(save_path, mode="wb") as fs_file:
            fs_file.write(bytes_buffer.getvalue())

        self._invalidate_cache()

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DataSetError:
            return False

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
