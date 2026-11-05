"""``PartitionedDataset`` loads and saves partitioned file-like data using the
underlying dataset definition. It also uses `fsspec` for filesystem level operations.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse
from warnings import warn

import fsspec
from kedro.io.catalog_config_resolver import CREDENTIALS_KEY
from kedro.io.core import (
    VERSION_KEY,
    AbstractDataset,
    DatasetError,
    parse_dataset_definition,
)

from kedro_datasets._utils import validate_sub_path

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
        last_three_parts = path_obj.relative_to(path_obj.parents[2])
        raise DatasetError(
            f"`{path}` is not a well-formed versioned path ending with "
            f"`filename/timestamp/filename` (got `{last_three_parts}`)."
        )
    return str(grandparent)


class PartitionedDataset(AbstractDataset[dict[str, Any], dict[str, Callable[[], Any]]]):
    """``PartitionedDataset`` loads and saves partitioned file-like data using the
    underlying dataset definition. For filesystem level operations it uses `fsspec`:
    https://github.com/intake/filesystem_spec.

    It also supports advanced features like [lazy saving](https://docs.kedro.org/en/stable/catalog-data/partitioned_and_incremental_datasets/#partitioned-dataset-lazy-saving).

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        station_data:
          type: partitions.PartitionedDataset
          path: data/03_primary/station_data
          dataset:
            type: pandas.CSVDataset
            load_args:
              sep: '\\t'
            save_args:
              sep: '\\t'
              index: true
          filename_suffix: '.dat'
          save_lazily: True
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

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
        >>> # Save it as small partitions with DAY_OF_MONTH as the partition key
        >>> dataset = PartitionedDataset(
        ...     path=str(tmp_path / "df_with_partition"),
        ...     dataset="pandas.CSVDataset",
        ...     filename_suffix=".csv",
        ...     save_lazily=False,
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
        >>> # print(partition_data)

        You can also load multiple partitions from a remote storage and combine them
        like this:

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
        path: str | os.PathLike,
        dataset: str | type[AbstractDataset] | dict[str, Any],
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: dict[str, Any] | None = None,
        load_args: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        overwrite: bool = False,
        save_lazily: bool = True,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Creates a new instance of ``PartitionedDataset``.

        Args:
            path: Path to the folder containing partitioned data. Accepts strings
                and ``os.PathLike`` objects.
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
                d) a dictionary mapping file extensions (e.g. ``{".csv": "pandas.CSVDataset", ".json": "json.JSONDataset"}``)
                to definitions from a), b), or c), to support multiple underlying dataset types.
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
                https://docs.kedro.org/en/stable/data/partitioned_and_incremental_datasets.html#partitioned-dataset-credentials
            load_args: Keyword arguments to be passed into ``find()`` method of
                the filesystem implementation.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
            overwrite: If True, any existing partitions will be removed.
            save_lazily: Parameter to enable/disable lazy saving, the default is True. Meaning that if callable object
                is passed as data to save, the partition’s data will not be materialised until it is time to write.
                Lazy saving example:
                https://docs.kedro.org/en/stable/catalog-data/partitioned_and_incremental_datasets/#partitioned-dataset-lazy-saving
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled for the underlying dataset.
        """
        # for performance reasons
        from fsspec.utils import infer_storage_options  # noqa: PLC0415

        super().__init__()

        self._path = os.fspath(path)
        self._filename_suffix = filename_suffix
        self._overwrite = overwrite
        self._protocol = infer_storage_options(self._path)["protocol"]
        self._cached_partitions: list[str] | None = None
        self._save_lazily = save_lazily
        self.metadata = metadata

        self._is_multi_dataset = (
            isinstance(dataset, dict) and "type" not in dataset and bool(dataset)
        )
        self._credentials = deepcopy(credentials or {})
        self._fs_args = deepcopy(fs_args or {})
        self._filepath_arg = filepath_arg

        if not self._is_multi_dataset:
            self._init_single_dataset(dataset)
        else:
            assert isinstance(dataset, dict)
            self._init_multi_dataset(dataset)

        self._load_args = deepcopy(load_args or {})
        self._sep = self._filesystem.sep
        # since some filesystem implementations may implement a global cache
        self._invalidate_caches()

    def _init_single_dataset(
        self, dataset: str | type[AbstractDataset] | dict[str, Any]
    ) -> None:
        dataset_def = dataset if isinstance(dataset, dict) else {"type": dataset}
        self._dataset_type, self._dataset_config = parse_dataset_definition(dataset_def)

        if self._credentials:
            if CREDENTIALS_KEY in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": CREDENTIALS_KEY, "target": "underlying dataset"},
                )
            else:
                self._dataset_config[CREDENTIALS_KEY] = deepcopy(self._credentials)

        if self._fs_args:
            if "fs_args" in self._dataset_config:
                self._logger.warning(
                    KEY_PROPAGATION_WARNING,
                    {"keys": "filesystem arguments", "target": "underlying dataset"},
                )
            else:
                self._dataset_config["fs_args"] = deepcopy(self._fs_args)

        if self._filepath_arg in self._dataset_config:
            warn(
                f"'{self._filepath_arg}' key must not be specified in the dataset "
                f"definition as it will be overwritten by partition path"
            )

    def _init_multi_dataset(self, dataset: dict[str, Any]) -> None:
        self._dataset_type = AbstractDataset  # type: ignore[type-abstract]
        self._dataset_config = {}
        self._dataset_configs: dict[
            str, tuple[type[AbstractDataset], dict[str, Any]]
        ] = {}

        for pattern, sub_dataset in dataset.items():
            pattern_key = (
                pattern
                if (pattern.startswith(".") or pattern == "*")
                else f".{pattern}"
            )
            sub_def = (
                sub_dataset if isinstance(sub_dataset, dict) else {"type": sub_dataset}
            )
            sub_type, sub_config = parse_dataset_definition(sub_def)

            if self._credentials:
                if CREDENTIALS_KEY in sub_config:
                    self._logger.warning(
                        KEY_PROPAGATION_WARNING,
                        {
                            "keys": CREDENTIALS_KEY,
                            "target": f"underlying dataset for '{pattern_key}'",
                        },
                    )
                else:
                    sub_config[CREDENTIALS_KEY] = deepcopy(self._credentials)

            if self._fs_args:
                if "fs_args" in sub_config:
                    self._logger.warning(
                        KEY_PROPAGATION_WARNING,
                        {
                            "keys": "filesystem arguments",
                            "target": f"underlying dataset for '{pattern_key}'",
                        },
                    )
                else:
                    sub_config["fs_args"] = deepcopy(self._fs_args)

            if self._filepath_arg in sub_config:
                warn(
                    f"'{self._filepath_arg}' key must not be specified in the dataset "
                    f"definition for '{pattern_key}' as it will be overwritten by partition path"
                )

            self._dataset_configs[pattern_key] = (sub_type, sub_config)

    @property
    def _filesystem(self):
        protocol = "s3" if self._protocol in S3_PROTOCOLS else self._protocol
        return fsspec.filesystem(protocol, **self._credentials, **self._fs_args)

    @property
    def _normalized_path(self) -> str:
        if self._protocol in S3_PROTOCOLS:
            return urlparse(self._path)._replace(scheme="s3").geturl()
        return self._path

    def _get_dataset_config_for_path(
        self, path: str
    ) -> tuple[type[AbstractDataset], dict[str, Any]]:
        if not self._is_multi_dataset:
            return self._dataset_type, self._dataset_config  # type: ignore

        sorted_patterns = sorted(
            [k for k in self._dataset_configs if k != "*"],
            key=len,
            reverse=True,
        )
        for pattern in sorted_patterns:
            if path.endswith(pattern):
                return self._dataset_configs[pattern]

        if "*" in self._dataset_configs:
            return self._dataset_configs["*"]

        configured = list(self._dataset_configs.keys())
        raise DatasetError(
            f"No matching dataset definition found for partition path '{path}'. "
            f"Configured extensions/patterns: {configured}"
        )

    def _list_partitions(self) -> list[str]:
        if self._cached_partitions is not None:
            return self._cached_partitions

        files = self._filesystem.find(self._normalized_path, **self._load_args)

        if not self._is_multi_dataset:
            dataset_is_versioned = VERSION_KEY in self._dataset_config  # type: ignore
            self._cached_partitions = [
                _grandparent(path) if dataset_is_versioned else path
                for path in files
                if path.endswith(self._filename_suffix)
            ]
            return self._cached_partitions

        sorted_patterns = sorted(
            [k for k in self._dataset_configs if k != "*"],
            key=len,
            reverse=True,
        )
        has_wildcard = "*" in self._dataset_configs

        matched = []
        for path in files:
            if not path.endswith(self._filename_suffix):
                continue
            matching_pattern = None
            for pattern in sorted_patterns:
                if path.endswith(pattern):
                    matching_pattern = pattern
                    break
            if matching_pattern is None and has_wildcard:
                matching_pattern = "*"

            if matching_pattern is not None:
                sub_type, sub_config = self._dataset_configs[matching_pattern]
                dataset_is_versioned = VERSION_KEY in sub_config
                matched.append(_grandparent(path) if dataset_is_versioned else path)

        self._cached_partitions = matched
        return self._cached_partitions

    def _join_protocol(self, path: str) -> str:
        protocol_prefix = f"{self._protocol}://"
        if self._path.startswith(protocol_prefix) and not path.startswith(
            protocol_prefix
        ):
            return f"{protocol_prefix}{path}"
        return path

    def _partition_to_path(self, path: str):
        dir_path = self._filesystem._strip_protocol(self._normalized_path).rstrip(
            self._sep
        )

        # Validate the path is within the base directory
        validate_sub_path(path, dir_path)

        full_path = self._sep.join([dir_path, path])
        return full_path + self._filename_suffix

    def _path_to_partition(self, path: str) -> str:
        dir_path = self._filesystem._strip_protocol(self._normalized_path).rstrip(
            self._sep
        )
        path = path.split(dir_path, 1).pop().lstrip(self._sep)
        if self._filename_suffix and path.endswith(self._filename_suffix):
            path = path[: -len(self._filename_suffix)]

        # Validate the partition ID to ensure it doesn't escape the base directory
        validate_sub_path(path, dir_path)

        return path

    def load(self) -> dict[str, Callable[[], Any]]:
        self._invalidate_caches()  # Invalidate cache before listing partitions

        partitions = {}

        # The _list_partitions() call will now always perform a fresh scan
        # because its cache was just cleared.
        for partition_file_path in self._list_partitions():
            sub_type, sub_config = self._get_dataset_config_for_path(
                partition_file_path
            )
            kwargs = deepcopy(sub_config)
            # join the protocol back since PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition_file_path)
            dataset = sub_type(**kwargs)  # type: ignore
            partition_id = self._path_to_partition(partition_file_path)
            partitions[partition_id] = dataset.load

        if not partitions:
            raise DatasetError(f"No partitions found in '{self._path}'")

        return partitions

    def save(self, data: dict[str, Any]) -> None:
        if self._overwrite and self._filesystem.exists(self._normalized_path):
            self._filesystem.rm(self._normalized_path, recursive=True)

        for partition_id, partition_data in sorted(data.items()):
            partition = self._partition_to_path(partition_id)
            sub_type, sub_config = self._get_dataset_config_for_path(partition)
            kwargs = deepcopy(sub_config)
            # join the protocol back since tools like PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            dataset = sub_type(**kwargs)  # type: ignore
            if callable(partition_data) and self._save_lazily:
                partition_data = partition_data()  # noqa: PLW2901
            dataset.save(partition_data)
        self._invalidate_caches()

    def _describe(self) -> dict[str, Any]:
        if not self._is_multi_dataset:
            clean_dataset_config = (
                {k: v for k, v in self._dataset_config.items() if k != CREDENTIALS_KEY}
                if isinstance(self._dataset_config, dict)
                else self._dataset_config
            )
            return {
                "path": self._path,
                "dataset_type": self._dataset_type.__name__,  # type: ignore
                "dataset_config": clean_dataset_config,
            }

        clean_configs = {}
        for pattern, (sub_type, sub_config) in self._dataset_configs.items():
            clean_sub = (
                {k: v for k, v in sub_config.items() if k != CREDENTIALS_KEY}
                if isinstance(sub_config, dict)
                else sub_config
            )
            clean_configs[pattern] = {
                "dataset_type": sub_type.__name__,
                "dataset_config": clean_sub,
            }
        return {
            "path": self._path,
            "dataset_configs": clean_configs,
        }

    def __repr__(self) -> str:
        object_description = self._describe()

        if not self._is_multi_dataset:
            # Dummy object to call _pretty_repr
            # Only clean_dataset_config parameters are exposed
            kwargs = deepcopy(self._dataset_config)
            kwargs[self._filepath_arg] = ""
            dataset = self._dataset_type(**kwargs)  # type: ignore

            object_description_repr = {
                "filepath": object_description["path"],
                "dataset": dataset._pretty_repr(object_description["dataset_config"]),
            }
            return self._pretty_repr(object_description_repr)

        dataset_reprs = {}
        for pattern, (sub_type, sub_config) in self._dataset_configs.items():
            kwargs = deepcopy(sub_config)
            kwargs[self._filepath_arg] = ""
            sub_dataset = sub_type(**kwargs)  # type: ignore
            clean_sub = object_description["dataset_configs"][pattern]["dataset_config"]
            dataset_reprs[pattern] = sub_dataset._pretty_repr(clean_sub)

        object_description_repr = {
            "filepath": object_description["path"],
            "dataset": dataset_reprs,
        }
        return self._pretty_repr(object_description_repr)

    def _invalidate_caches(self) -> None:
        self._cached_partitions = None
        self._filesystem.invalidate_cache(self._normalized_path)

    def _exists(self) -> bool:
        return bool(self._list_partitions())

    def _release(self) -> None:
        super()._release()
        self._invalidate_caches()
