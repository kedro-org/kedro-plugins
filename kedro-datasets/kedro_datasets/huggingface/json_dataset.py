from __future__ import annotations

from typing import ClassVar

from ._base import FilesystemDataset


class JSONDataset(FilesystemDataset):
    """``JSONDataset`` loads/saves Hugging Face ``Dataset`` and
    ``DatasetDict`` objects to/from JSON files.

    Iterable variants (``IterableDataset``, ``IterableDatasetDict``)
    are materialised before saving.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        reviews:
          type: huggingface.JSONDataset
          path: data/01_raw/reviews.json
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.json_dataset import (
        ...     JSONDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict(
        ...     {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        ... )
        >>>
        >>> dataset = JSONDataset(
        ...     path=tmp_path / "test_hf_dataset.json"
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert reloaded.to_dict() == data.to_dict()
    """

    BUILDER: ClassVar[str] = "json"
    EXTENSION: ClassVar[str] = ".json"
