from __future__ import annotations

from typing import ClassVar

from ._base import FilesystemDataset


class CSVDataset(FilesystemDataset):
    """``CSVDataset`` loads/saves Hugging Face ``Dataset`` and
    ``DatasetDict`` objects to/from CSV files.

    Iterable variants (``IterableDataset``, ``IterableDatasetDict``)
    are materialised before saving.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        reviews:
          type: huggingface.CSVDataset
          path: data/01_raw/reviews.csv
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/):

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.csv_dataset import (
        ...     CSVDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict(
        ...     {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
        ... )
        >>>
        >>> dataset = CSVDataset(
        ...     path=tmp_path / "test_hf_dataset.csv"
        ... )
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> assert reloaded.to_dict() == data.to_dict()
    """

    BUILDER: ClassVar[str] = "csv"
    EXTENSION: ClassVar[str] = ".csv"
