"""``CSVDataset`` is a data set used to load and save data to CSV files using Dask
dataframe"""
from __future__ import annotations

from copy import deepcopy
from typing import Any

import dask.dataframe as dd
import fsspec
from kedro.io.core import AbstractDataset, get_protocol_and_path


class CSVDataset(AbstractDataset[dd.DataFrame, dd.DataFrame]):
    """``CSVDataset`` loads and saves data to comma-separated value file(s). It uses Dask
    remote data services to handle the corresponding load and save operations:
    https://docs.dask.org/en/latest/how-to/connect-to-remote-data.html

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: dask.CSVDataset
          filepath: s3://bucket_name/path/to/folder
          save_args:
            compression: GZIP
          credentials:
            client_kwargs:
              aws_access_key_id: YOUR_KEY
              aws_secret_access_key: YOUR_SECRET

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.dask import CSVDataset
        >>> import pandas as pd
        >>> import dask.dataframe as dd
        >>> data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [[5, 6], [7, 8]]})
        >>> ddf = dd.from_pandas(data, npartitions=1)
        >>> dataset = CSVDataset(filepath="path/to/folder/*.csv")
        >>> dataset.save(ddf)
        >>> reloaded = dataset.load()
        >>> assert ddf.compute().equals(reloaded.compute())
    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {"index": False}

    def __init__(  # noqa: PLR0913
        self,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``CSVDataset`` pointing to concrete
        CSV files.

        Args:
            filepath: Filepath in POSIX format to a CSV file
                CSV collection or the directory of a multipart CSV.
            load_args: Additional loading options `dask.dataframe.read_csv`:
                https://docs.dask.org/en/latest/generated/dask.dataframe.read_csv.html
            save_args: Additional saving options for `dask.dataframe.to_csv`:
                https://docs.dask.org/en/latest/generated/dask.dataframe.to_csv.html
            credentials: Credentials required to get access to the underlying filesystem.
                E.g. for ``GCSFileSystem`` it should look like `{"token": None}`.
            fs_args: Optional parameters to the backend file system driver:
                https://docs.dask.org/en/latest/how-to/connect-to-remote-data.html#optional-parameters
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        self._filepath = filepath
        self._fs_args = deepcopy(fs_args) or {}
        self._credentials = deepcopy(credentials) or {}

        self.metadata = metadata

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    @property
    def fs_args(self) -> dict[str, Any]:
        """Property of optional file system parameters.

        Returns:
            A dictionary of backend file system parameters, including credentials.
        """
        fs_args = deepcopy(self._fs_args)
        fs_args.update(self._credentials)
        return fs_args

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def _load(self) -> dd.DataFrame:
        return dd.read_csv(
            self._filepath, storage_options=self.fs_args, **self._load_args
        )

    def _save(self, data: dd.DataFrame) -> None:
        data.to_csv(self._filepath, storage_options=self.fs_args, **self._save_args)

    def _exists(self) -> bool:
        protocol = get_protocol_and_path(self._filepath)[0]
        file_system = fsspec.filesystem(protocol=protocol, **self.fs_args)
        return file_system.exists(self._filepath)
