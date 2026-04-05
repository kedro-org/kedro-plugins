from __future__ import annotations

import os
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, TypeAlias

import fsspec
from datasets import (
    Dataset,
    DatasetDict,
    IterableDataset,
    IterableDatasetDict,
    load_dataset,
    load_from_disk,
)
from huggingface_hub import HfApi
from kedro.io import AbstractDataset
from kedro.io.core import (
    AbstractVersionedDataset,
    DatasetError,
    Version,
    get_filepath_str,
    get_protocol_and_path,
)

DatasetLike: TypeAlias = Dataset | DatasetDict | IterableDataset | IterableDatasetDict


class HFDataset(AbstractDataset[None, DatasetLike]):
    """``HFDataset`` loads Hugging Face datasets
    using the `datasets <https://pypi.org/project/datasets>`_ library.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        yelp_reviews:
          type: kedro_hf_datasets.HFDataset
          dataset_name: yelp_review_full
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from datasets.utils.logging import ERROR, disable_progress_bar, set_verbosity
        >>> from kedro_datasets.huggingface import HFDataset
        >>>
        >>> disable_progress_bar()  # for doctest to pass
        >>> set_verbosity(ERROR)  # for doctest to pass
        >>>
        >>> dataset = HFDataset(dataset_name="openai_humaneval")
        >>> ds = dataset.load()  # doctest: +SKIP
        >>> assert "test" in ds  # doctest: +SKIP
        >>> assert len(ds["test"]) == 164  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        dataset_name: str,
        dataset_kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.dataset_name = dataset_name
        self._dataset_kwargs = dataset_kwargs or {}
        self.metadata = metadata

    def load(self) -> DatasetLike:
        # TODO: Replace suppression with the solution from here: https://github.com/kedro-org/kedro-plugins/issues/1131
        return load_dataset(self.dataset_name, **self._dataset_kwargs)  # nosec

    def save(self):
        raise NotImplementedError("Not yet implemented")

    def _describe(self) -> dict[str, Any]:
        api = HfApi()
        dataset_info = list(api.list_datasets(search=self.dataset_name))[0]
        return {
            "dataset_name": self.dataset_name,
            "dataset_tags": dataset_info.tags,
            "dataset_author": dataset_info.author,
        }

    @staticmethod
    def list_datasets():
        api = HfApi()
        return list(api.list_datasets())


class LocalHFDataset(AbstractVersionedDataset[DatasetLike, DatasetLike]):
    """``LocalHFDataset`` loads/saves Hugging Face ``Dataset``,
    ``DatasetDict``, ``IterableDataset``, and
    ``IterableDatasetDict`` objects to/from disk using an
    underlying filesystem (e.g.: local, S3, GCS). Iterable
    variants are materialized before saving.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        with a ``datasets.Dataset``

        ```yaml
        reviews:
          type: huggingface.LocalHFDataset
          path: data/01_raw/reviews.arrow
        ```

        By default, data will be loaded and saved from
        [Arrow](https://huggingface.co/docs/datasets/about_arrow) format.

        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        with a ``datasets.DatasetDict`` in JSON format:

        ```yaml
        review_dict:
            type: huggingface.LocalHFDataset
            path: data/01_raw/review_dict/
            file_format: json
        ```

        This saves each individual ``datasets.Dataset`` into separate files
        in the directory in JSON format.

        The ``file_format`` accepts `arrow`, `parquet`, `json`, `csv`, `lance`,
        and `hdf5` as arguments.

        For more on saving and loading from a filesystem with the Datasets
        library, see
        [here](https://huggingface.co/docs/datasets/v4.8.4/en/loading#local-and-remote-files).

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        with a ``datasets.Dataset``:

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.hugging_face_dataset import (
        ...     LocalHFDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict(
        ...     {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        ... )
        >>>
        >>> dataset = LocalHFDataset(
        ...     path=tmp_path / "test_hf_dataset.arrow"
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert reloaded.to_dict() == data.to_dict()

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        with a ``datasets.DatasetDict``:

        >>> from datasets import Dataset, DatasetDict
        >>> from kedro_datasets.huggingface.hugging_face_dataset import (
        ...     LocalHFDataset,
        ... )
        >>>
        >>> data = DatasetDict({
        ...     "train": Dataset.from_dict(
        ...         {"col1": [1, 2], "col2": ["a", "b"]}
        ...     ),
        ...     "test": Dataset.from_dict(
        ...         {"col1": [3], "col2": ["c"]}
        ...     ),
        ... })
        >>>
        >>> dataset = LocalHFDataset(
        ...     path=tmp_path / "test_hf_dataset_dict"
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert list(reloaded.keys()) == ["train", "test"]

    """

    _SUPPORTED_FORMATS = {"arrow", "parquet", "json", "csv", "lance", "hdf5"}
    _FORMAT_EXTENSIONS = {
        "arrow": ".arrow",
        "parquet": ".parquet",
        "json": ".json",
        "csv": ".csv",
        "lance": ".lance",
        "hdf5": ".h5",
    }

    def __init__(  # noqa: PLR0913
        self,
        *,
        path: str | os.PathLike,
        file_format: str = "arrow",
        version: Version | None = None,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        credentials: dict[str, Any] | None = None,
        fs_args: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if file_format not in self._SUPPORTED_FORMATS:
            msg = (
                f"Unsupported file_format '{file_format}'. "
                f"Must be one of {sorted(self._SUPPORTED_FORMATS)}."
            )
            raise DatasetError(msg)

        self._file_format = file_format
        _fs_args = deepcopy(fs_args) or {}
        _credentials = deepcopy(credentials) or {}

        protocol, resolved_path = get_protocol_and_path(path, version)
        self._protocol = protocol

        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)

        self._fs = fsspec.filesystem(self._protocol, **_credentials, **_fs_args)

        self._load_args = load_args or {}
        self._save_args = save_args or {}
        self.metadata = metadata

        # storage_options passed to HF's load/save methods
        self._storage_options = {**_credentials, **_fs_args} or None

        super().__init__(
            filepath=PurePosixPath(resolved_path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

    def _load(self) -> DatasetLike:
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        if self._file_format == "arrow":
            return load_from_disk(
                load_path,
                storage_options=self._storage_options,
                **self._load_args,
            )

        ext = self._FORMAT_EXTENSIONS[self._file_format]
        loader = getattr(Dataset, f"from_{self._file_format}")

        if self._fs.isdir(load_path):
            paths = {
                PurePosixPath(p).stem: p for p in self._fs.glob(f"{load_path}/*{ext}")
            }
            return DatasetDict(
                {
                    split: loader(path, **self._load_args)
                    for split, path in paths.items()
                }
            )

        return loader(load_path, **self._load_args)

    def _save(self, data: DatasetLike) -> None:
        if not isinstance(
            data,
            Dataset | DatasetDict | IterableDataset | IterableDatasetDict,
        ):
            msg = (
                "LocalHFDataset only supports `datasets.Dataset`, "
                "`datasets.DatasetDict`, "
                "`datasets.IterableDataset`, and "
                "`datasets.IterableDatasetDict` instances. "
                f"Got {type(data)}"
            )
            raise DatasetError(msg)

        if isinstance(data, IterableDatasetDict):
            data = DatasetDict({k: Dataset.from_list(list(v)) for k, v in data.items()})
        elif isinstance(data, IterableDataset):
            data = Dataset.from_list(list(data))

        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        if self._file_format == "arrow":
            data.save_to_disk(
                save_path,
                storage_options=self._storage_options,
                **self._save_args,
            )
        elif isinstance(data, DatasetDict):
            self._fs.mkdirs(save_path, exist_ok=True)
            ext = self._FORMAT_EXTENSIONS[self._file_format]
            saver = f"to_{self._file_format}"
            for split, split_ds in data.items():
                split_path = f"{save_path}/{split}{ext}"
                getattr(split_ds, saver)(
                    split_path,
                    storage_options=self._storage_options,
                    **self._save_args,
                )
        else:
            saver = f"to_{self._file_format}"
            getattr(data, saver)(
                save_path,
                storage_options=self._storage_options,
                **self._save_args,
            )

        self._invalidate_cache()

    def _describe(self) -> dict[str, Any]:
        return {
            "path": self._filepath,
            "file_format": self._file_format,
            "protocol": self._protocol,
            "version": self._version,
            "load_args": self._load_args,
            "save_args": self._save_args,
        }

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        if self._file_format == "arrow":
            return self._fs.isdir(load_path) and (
                self._fs.exists(f"{load_path}/dataset_dict.json")
                or self._fs.exists(f"{load_path}/dataset_info.json")
            )

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        path = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(path)
