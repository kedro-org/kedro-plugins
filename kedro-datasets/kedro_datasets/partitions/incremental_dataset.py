"""``IncrementalDataset`` inherits from ``PartitionedDataset``, which loads
and saves partitioned file-like data using the underlying dataset
definition. ``IncrementalDataset`` also stores the information about the last
processed partition in so-called `checkpoint` that is persisted to the location
of the data partitions by default, so that subsequent pipeline run loads only
new partitions past the checkpoint.It also uses `fsspec` for filesystem level operations.
"""
from __future__ import annotations

import operator
from copy import deepcopy
from typing import Any, Callable

from cachetools import cachedmethod
from kedro.io.core import (
    VERSION_KEY,
    VERSIONED_FLAG_KEY,
    AbstractDataset,
    DatasetError,
    parse_dataset_definition,
)
from kedro.io.data_catalog import CREDENTIALS_KEY
from kedro.utils import load_obj

from .partitioned_dataset import (
    KEY_PROPAGATION_WARNING,
    PartitionedDataset,
    _grandparent,
)


class IncrementalDataset(PartitionedDataset):
    """``IncrementalDataset`` inherits from ``PartitionedDataset``, which loads
    and saves partitioned file-like data using the underlying dataset
    definition. For filesystem level operations it uses `fsspec`:
    https://github.com/intake/filesystem_spec. ``IncrementalDataset`` also stores
    the information about the last processed partition in so-called `checkpoint`
    that is persisted to the location of the data partitions by default, so that
    subsequent pipeline run loads only new partitions past the checkpoint.

    Example:

    .. code-block:: pycon

        >>> from kedro_datasets.partitions import IncrementalDataset
        >>>
        >>> dataset = IncrementalDataset(path=str(tmp_path/ "test_data"), dataset="pandas.CSVDataset")
        >>> loaded = dataset.load()  # loads all available partitions
        >>> # assert isinstance(loaded, dict)
        >>>
        >>> dataset.confirm()  # update checkpoint value to the last processed partition ID
        >>> reloaded = dataset.load()  # still loads all available partitions
        >>>
        >>> dataset.release()  # clears load cache
        >>> # returns an empty dictionary as no new partitions were added
        >>> assert dataset.load() == {}
    """

    DEFAULT_CHECKPOINT_TYPE = "kedro_datasets.text.TextDataset"
    DEFAULT_CHECKPOINT_FILENAME = "CHECKPOINT"

    def __init__(  # noqa: PLR0913
        self,
        *,
        path: str,
        dataset: str | type[AbstractDataset] | dict[str, Any],
        checkpoint: str | dict[str, Any] | None = None,
        filepath_arg: str = "filepath",
        filename_suffix: str = "",
        credentials: dict[str, Any] = None,
        load_args: dict[str, Any] = None,
        fs_args: dict[str, Any] = None,
        metadata: dict[str, Any] = None,
    ) -> None:
        """Creates a new instance of ``IncrementalDataset``.

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
            checkpoint: Optional checkpoint configuration. Accepts a dictionary
                with the corresponding dataset definition including ``filepath``
                (unlike ``dataset`` argument). Checkpoint configuration is
                described here:
                https://kedro.readthedocs.io/en/stable/data/kedro_io.html#checkpoint-configuration
                Credentials for the checkpoint can be explicitly specified
                in this configuration.
            filepath_arg: Underlying dataset initializer argument that will
                contain a path to each corresponding partition file.
                If unspecified, defaults to "filepath".
            filename_suffix: If specified, only partitions that end with this
                string will be processed.
            credentials: Protocol-specific options that will be passed to
                ``fsspec.filesystem``
                https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.filesystem,
                the dataset dataset initializer and the checkpoint. If
                the dataset or the checkpoint configuration contains explicit
                credentials spec, then such spec will take precedence.
                All possible credentials management scenarios are documented here:
                https://kedro.readthedocs.io/en/stable/data/kedro_io.html#partitioned-dataset-credentials
            load_args: Keyword arguments to be passed into ``find()`` method of
                the filesystem implementation.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.

        Raises:
            DatasetError: If versioning is enabled for the checkpoint dataset.
        """

        super().__init__(
            path=path,
            dataset=dataset,
            filepath_arg=filepath_arg,
            filename_suffix=filename_suffix,
            credentials=credentials,
            load_args=load_args,
            fs_args=fs_args,
        )

        self._checkpoint_config = self._parse_checkpoint_config(checkpoint)
        self._force_checkpoint = self._checkpoint_config.pop("force_checkpoint", None)
        self.metadata = metadata

        comparison_func = self._checkpoint_config.pop("comparison_func", operator.gt)
        if isinstance(comparison_func, str):
            comparison_func = load_obj(comparison_func)
        self._comparison_func = comparison_func

    def _parse_checkpoint_config(
        self, checkpoint_config: str | dict[str, Any] | None
    ) -> dict[str, Any]:
        checkpoint_config = deepcopy(checkpoint_config)
        if isinstance(checkpoint_config, str):
            checkpoint_config = {"force_checkpoint": checkpoint_config}
        checkpoint_config = checkpoint_config or {}

        for key in {VERSION_KEY, VERSIONED_FLAG_KEY} & checkpoint_config.keys():
            raise DatasetError(
                f"'{self.__class__.__name__}' does not support versioning of the "
                f"checkpoint. Please remove '{key}' key from the checkpoint definition."
            )

        default_checkpoint_path = self._sep.join(
            [self._normalized_path.rstrip(self._sep), self.DEFAULT_CHECKPOINT_FILENAME]
        )
        default_config = {
            "type": self.DEFAULT_CHECKPOINT_TYPE,
            self._filepath_arg: default_checkpoint_path,
        }
        if self._credentials:
            default_config[CREDENTIALS_KEY] = deepcopy(self._credentials)

        if CREDENTIALS_KEY in default_config.keys() & checkpoint_config.keys():
            self._logger.warning(
                KEY_PROPAGATION_WARNING,
                {"keys": CREDENTIALS_KEY, "target": "checkpoint"},
            )

        return {**default_config, **checkpoint_config}

    @cachedmethod(cache=operator.attrgetter("_partition_cache"))
    def _list_partitions(self) -> list[str]:
        checkpoint = self._read_checkpoint()
        checkpoint_path = self._filesystem._strip_protocol(
            self._checkpoint_config[self._filepath_arg]
        )
        dataset_is_versioned = VERSION_KEY in self._dataset_config

        def _is_valid_partition(partition) -> bool:
            if not partition.endswith(self._filename_suffix):
                return False
            if partition == checkpoint_path:
                return False
            if checkpoint is None:
                # nothing was processed yet
                return True
            partition_id = self._path_to_partition(partition)
            return self._comparison_func(partition_id, checkpoint)

        return sorted(
            _grandparent(path) if dataset_is_versioned else path
            for path in self._filesystem.find(self._normalized_path, **self._load_args)
            if _is_valid_partition(path)
        )

    @property
    def _checkpoint(self) -> AbstractDataset:
        type_, kwargs = parse_dataset_definition(self._checkpoint_config)
        return type_(**kwargs)  # type: ignore

    def _read_checkpoint(self) -> str | None:
        if self._force_checkpoint is not None:
            return self._force_checkpoint
        try:
            return self._checkpoint.load()
        except DatasetError:
            return None

    def _load(self) -> dict[str, Callable[[], Any]]:
        partitions: dict[str, Any] = {}

        for partition in self._list_partitions():
            partition_id = self._path_to_partition(partition)
            kwargs = deepcopy(self._dataset_config)
            # join the protocol back since PySpark may rely on it
            kwargs[self._filepath_arg] = self._join_protocol(partition)
            partitions[partition_id] = self._dataset_type(  # type: ignore
                **kwargs
            ).load()

        return partitions

    def confirm(self) -> None:
        """Confirm the dataset by updating the checkpoint value to the latest
        processed partition ID"""
        partition_ids = [self._path_to_partition(p) for p in self._list_partitions()]
        if partition_ids:
            self._checkpoint.save(partition_ids[-1])  # checkpoint to last partition
