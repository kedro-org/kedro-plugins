from __future__ import annotations

from typing import Any, ClassVar

from datasets import Dataset, DatasetDict, load_from_disk
from kedro.io.core import DatasetError, get_filepath_str

from ._base import DatasetLike, FilesystemDataset


class ArrowDataset(FilesystemDataset):
    """``ArrowDataset`` loads/saves Hugging Face ``Dataset`` and
    ``DatasetDict`` objects to/from disk in
    `Arrow <https://huggingface.co/docs/datasets/about_arrow>`_ format
    using ``save_to_disk`` / ``load_from_disk``.

    Saving ``IterableDataset`` or ``IterableDatasetDict`` objects is not
    supported and will raise a ``DatasetError``. Materialize the iterable
    dataset into a ``Dataset`` or ``DatasetDict`` before saving.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        reviews:
          type: huggingface.ArrowDataset
          path: data/01_raw/reviews
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.arrow_dataset import (
        ...     ArrowDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict(
        ...     {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        ... )
        >>>
        >>> dataset = ArrowDataset(
        ...     path=tmp_path / "test_hf_dataset"
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert reloaded.to_dict() == data.to_dict()
    """

    BUILDER: ClassVar[str] = "arrow"
    EXTENSION: ClassVar[str] = ".arrow"

    def _load_dataset(self, load_path: str) -> DatasetLike:
        return load_from_disk(
            load_path,
            storage_options=self._storage_options,
            **self._load_args,
        )

    def _save_dataset(self, data: Dataset, save_path: str) -> None:
        data.save_to_disk(
            save_path,
            storage_options=self._storage_options,
            **self._save_args,
        )

    def _save_dataset_dict(self, data: DatasetDict, save_path: str) -> None:
        data.save_to_disk(
            save_path,
            storage_options=self._storage_options,
            **self._save_args,
        )

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.isdir(load_path) and (
            self._fs.exists(f"{load_path}/dataset_dict.json")
            or self._fs.exists(f"{load_path}/dataset_info.json")
        )
