"""``PartitionedDataset`` loads and saves partitioned file-like data using the
underlying dataset definition. It also uses `fsspec` for filesystem level operations.
"""
from __future__ import annotations

import operator
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import urlparse
from warnings import warn

import fsspec
from cachetools import Cache, cachedmethod
from kedro.io.core import (
    VERSION_KEY,
    AbstractDataset,
    DatasetError,
    parse_dataset_definition,
)
from kedro.io.data_catalog import CREDENTIALS_KEY

KEY_PROPAGATION_WARNING = (
    "Top-level %(keys)s will not propagate into the %(target)s since "
    "%(keys)s were explicitly defined in the %(target)s config."
)

S3_PROTOCOLS = ("s3", "s3a", "s3n")


def _grandparent(path: str) -> str:
    """Check and return the logical parent of the parent of the path."""
    path_obj = PurePosixPath(path)
    grandparent = path_obj.parents[1]
    if grandparent.name != path_obj.name:
        last_three_parts = path_obj.relative_to(*path_obj.parts[:-3])
        raise DatasetError(
            f"`{path}` is not a well-formed versioned path ending with "
            f"`filename/timestamp/filename` (got `{last_three_parts}`)."
        )
    return str(grandparent)


class PartitionedDataset(AbstractDataset[dict[str, Any], dict[str, Callable[[], Any]]]):
    """``PartitionedDataset`` loads and saves partitioned file-like data using the
    underlying dataset definition. For filesystem level operations it uses `fsspec`:
    https://github.com/intake/filesystem_spec.

    It also supports advanced features like
    `lazy saving <https://kedro.readthedocs.io/en/stable/data/\
    kedro_io.html#partitioned-dataset-lazy-saving>`_.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        station_data:
          type: PartitionedDataset
          path: data/03_primary/station_data
          dataset:
            type: pandas.CSVDataset
            load_args:
              sep: '\\t'
            save_args:
              sep: '\\t'
              index: true
          filename_suffix: '.dat'

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from kedro_datasets.partitions import PartitionedDataset
        >>>
        >>> # Create a fake pandas dataframe with 10 rows of data
        >>> df = pd.DataFrame([{"DAY_OF_MONTH": str(i), "VALUE": i} for i in range(1, 11)])
        >>>
        >>> # Convert it to a dict of pd.DataFrame with DAY_OF_MONTH as the dict key
        >>> dict_df = {
        ...     day_of_month: df[df["DAY_OF_MONTH"] == day_of_month]
        ...     for day_of_month in df["DAY_OF_MONTH"]
        ... }
        >>>
        >>> # Save it as small paritions with DAY_OF_MONTH as the partition key
        >>> dataset = PartitionedDataset(
        ...     path=tmp_path / "df_with_partition",
        ...     dataset="pandas.CSVDataset",
        ...     filename_suffix=".csv",
        ... )
        >>> # This will create a folder `df_with_partition` and save multiple files
        >>> # with the dict key + filename_suffix as filename, i.e. 1.csv, 2.csv etc.
        >>> dataset.save(dict_df)
        >>>
        >>> # This will create lazy load functions instead of loading data into memory immediately.
        >>> loaded = dataset.load()
        >>>
        >>> # Load all the partitions
        >>> for partition_id, partition_load_func in loaded.items():
        ...     # The actual function that loads the data
        ...     partition_data = partition_load_func()
        ...
        >>> # Add the processing logic for individual partition HERE
        >>> print(partition_data)

    You can also load multiple partitions from a remote storage and combine them
    like this:

    .. code-block:: pycon

        >>> import pandas as pd
        >>> from kedro_datasets.partitions import PartitionedDataset
        >>>
        >>> # these credentials will be passed to both 'fsspec.filesystem()' call
        >>> # and the dataset initializer
        >>> credentials = {"key1": "secret1", "key2": "secret2"}
        >>>
        >>> dataset = PartitionedDataset(
        ...     path="s3://bucket-name/path/to/folder",
        ...     dataset="pandas.CSVDataset",
        ...     credentials=credentials,
        ... )
        >>> loaded = dataset.load()
        >>> # assert isinstance(loaded, dict)
        >>>
        >>> combine_all = pd.DataFrame()
        >>>
        >>> for partition_id, partition_load_func in loaded.items():
        ...     partition_data = partition_load_func()
        ...     combine_all = pd.concat([combine_all, partition_data], ignore_index=True, sort=True)
        ...
        >>> new_data = pd.DataFrame({"new": [1, 2]})
        >>> # creates "s3://bucket-name/path/to/folder/new/partition.csv"
        >>> dataset.save({"new/partition.csv": new_data})

    """

    def __init__(  # noqa: PLR0913
        self,
        *,
        path: str,
        dataset: str | type[AbstractDataset] | dict[str, Any],
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: dict[str, Any] = None,
        load_args: dict[str, Any] = None,
        fs_args: dict[str, Any] = None,
        overwrite: bool = False,
        metadata: dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``PartitionedDataset``.

        Args:
            path: Path to the folder containing partitioned data.
                If path starts with the protocol (e.g., ``s3://``) then the
                corresponding ``fsspec`` concrete filesystem implementation will
                be used. If protocol is not specified,
                ``fsspec.implementations.local.LocalFileSystem`` will be used.
                **Note:** Some concrete implementations are bundled with ``fsspec``,
                while others (like ``s3`` or ``gcs``) must be installed separately
                prior to usage of the ``PartitionedDataset``.
            dataset: Underlying dataset definition. This is used to instantiate
                the dataset for each file located inside the ``path``.
                Accepted formats are:
                a) object of a class that inherits from ``AbstractDataset``
                b) a string representing a fully qualified class name to such class
                c) a dictionary with ``type`` key pointing to a string from b),
                other keys are passed to the Dataset initializer.
                Credentials for the dataset can be explicitly specified in
                this configuration.
            filepath_arg: Underlying dataset initializer argument that will
                contain a path to each corresponding partition file.
                If unspecified, defaults to "filepath".
            filename_suffix: If specified, only partitions that end with this
                string will be processed.
            credentials: Protocol-specific options that will be passed to
                ``fsspec.filesystem``
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.filesystem
                and the dataset initializer. If the dataset config contains
                explicit credentials spec, then such spec will take precedence.
                All possible credentials management scenarios are documented here:
                https://kedro.readthedocs.io/en/stable/data/kedro_io.html#partitioned-dataset-credentials
            load_args: Keyword arguments to be passed into ``find()`` method of
                the filesystem implementation.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
            overwrite: If True, any existing partitions will be removed.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled for the underlying dataset.
        """
        from fsspec.utils import infer_storage_options  # for performance reasons

        super().__init__()

        self._path = path
        self._filename_suffix = filename_suffix
        self._overwrite = overwrite
        self._protocol = infer_storage_options(self._path)["protocol"]
        self._partition_cache: Cache = Cache(maxsize=1)
        self.metadata = metadata

        dataset = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset)

        if credentials:
            if CREDENTIALS_KEY in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": CREDENTIALS_KEY, "target": "underlying dataset"},
                )
            else:
                self._dataset_config[CREDENTIALS_KEY] = deepcopy(credentials)

        self._credentials = deepcopy(credentials) or {}

        self._fs_args = deepcopy(fs_args) or {}
        if self._fs_args:
            if "fs_args" in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": "filesystem arguments", "target": "underlying dataset"},
                )
            else:
                self._dataset_config["fs_args"] = deepcopy(self._fs_args)

        self._filepath_arg = filepath_arg
        if self._filepath_arg in self._dataset_config:
            warn(
                f"'{self._filepath_arg}' key must not be specified in the dataset "
                f"definition as it will be overwritten by partition path"
            )

        self._load_args = deepcopy(load_args) or {}
        self._sep = self._filesystem.sep
        # since some filesystem implementations may implement a global cache
        self._invalidate_caches()

    @property
    def _filesystem(self):
        protocol = "s3" if self._protocol in S3_PROTOCOLS else self._protocol
        return fsspec.filesystem(protocol, **self._credentials, **self._fs_args)

    @property
    def _normalized_path(self) -> str:
        if self._protocol in S3_PROTOCOLS:
            return urlparse(self._path)._replace(scheme="s3").geturl()
        return self._path

    @cachedmethod(cache=operator.attrgetter("_partition_cache"))
    def _list_partitions(self) -> list[str]:
        dataset_is_versioned = VERSION_KEY in self._dataset_config
        return [
            _grandparent(path) if dataset_is_versioned else path
            for path in self._filesystem.find(self._normalized_path, **self._load_args)
            if path.endswith(self._filename_suffix)
        ]

    def _join_protocol(self, path: str) -> str:
        protocol_prefix = f"{self._protocol}://"
        if self._path.startswith(protocol_prefix) and not path.startswith(
            protocol_prefix
        ):
            return f"{protocol_prefix}{path}"
        return path

    def _partition_to_path(self, path: str):
        dir_path = self._path.rstrip(self._sep)
        path = path.lstrip(self._sep)
        full_path = self._sep.join([dir_path, path]) + self._filename_suffix
        return full_path

    def _path_to_partition(self, path: str) -> str:
        dir_path = self._filesystem._strip_protocol(self._normalized_path)
        path = path.split(dir_path, 1).pop().lstrip(self._sep)
        if self._filename_suffix and path.endswith(self._filename_suffix):
            path = path[: -len(self._filename_suffix)]
        return path

    def _load(self) -> dict[str, Callable[[], Any]]:
        partitions = {}

        for partition in self._list_partitions():
            kwargs = deepcopy(self._dataset_config)
            # join the protocol back since PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            dataset = self._dataset_type(**kwargs)  # type: ignore
            partition_id = self._path_to_partition(partition)
            partitions[partition_id] = dataset.load

        if not partitions:
            raise DatasetError(f"No partitions found in '{self._path}'")

        return partitions

    def _save(self, data: dict[str, Any]) -> None:
        if self._overwrite and self._filesystem.exists(self._normalized_path):
            self._filesystem.rm(self._normalized_path, recursive=True)

        for partition_id, partition_data in sorted(data.items()):
            kwargs = deepcopy(self._dataset_config)
            partition = self._partition_to_path(partition_id)
            # join the protocol back since tools like PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            dataset = self._dataset_type(**kwargs)  # type: ignore
            if callable(partition_data):
                partition_data = partition_data()  # noqa: PLW2901
            dataset.save(partition_data)
        self._invalidate_caches()

    def _describe(self) -> dict[str, Any]:
        clean_dataset_config = (
            {k: v for k, v in self._dataset_config.items() if k != CREDENTIALS_KEY}
            if isinstance(self._dataset_config, dict)
            else self._dataset_config
        )
        return {
            "path": self._path,
            "dataset_type": self._dataset_type.__name__,
            "dataset_config": clean_dataset_config,
        }

    def _invalidate_caches(self) -> None:
        self._partition_cache.clear()
        self._filesystem.invalidate_cache(self._normalized_path)

    def _exists(self) -> bool:
        return bool(self._list_partitions())

    def _release(self) -> None:
        super()._release()
        self._invalidate_caches()
