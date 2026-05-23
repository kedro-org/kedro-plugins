from __future__ import annotations

from typing import ClassVar

from ._base import FilesystemDataset


class ParquetDataset(FilesystemDataset):
    """``ParquetDataset`` loads/saves Hugging Face ``Dataset`` and
    ``DatasetDict`` objects to/from
    `Parquet <https://parquet.apache.org/>`_ files.

    Saving ``IterableDataset`` or ``IterableDatasetDict`` objects is not
    supported and will raise a ``DatasetError``. Materialize the iterable
    dataset into a ``Dataset`` or ``DatasetDict`` before saving.

    Note that ``datasets`` loads a single file as a ``datasets.DatasetDict``
    with a single key called ``"train"``. You can get around this by specifying
    ``split`` in the ``load_args``. See examples for more info.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        to load a single file. Will be loaded as a ``datasets.DatasetDict`` with a single key
        ``"train"``:

        ```yaml
        reviews:
          type: huggingface.ParquetDataset
          path: data/01_raw/reviews.parquet
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.DatasetDict`` from a single file:

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.parquet_dataset import (
        ...     ParquetDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        >>> dataset = ParquetDataset(path=tmp_path / "data.parquet")
        >>> dataset.save(data)
        >>> loaded = dataset.load()
        >>> assert "train" in loaded

        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        to load a ``datasets.Dataset`` from a single file:

        ```yaml
        reviews:
          type: huggingface.ParquetDataset
          path: data/01_raw/reviews.parquet
          load_args:
            split: train
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.Dataset`` from a single file:

        >>> from datasets import Dataset
        >>> from kedro_datasets.huggingface.parquet_dataset import (
        ...     ParquetDataset,
        ... )
        >>>
        >>> data = Dataset.from_dict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        >>> dataset = ParquetDataset(
        ...     path=tmp_path / "data.parquet",
        ...     load_args={"split": "train"},
        ... )
        >>> dataset.save(data)
        >>> loaded = dataset.load()
        >>> assert type(loaded.shape) is tuple  # No "train" key.

        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        to load a ``datasets.DatasetDict`` from a directory of files:

        ```yaml
        reviews:
          type: huggingface.ParquetDataset
          path: data/01_raw/reviews
          data_files:
            labels: labels.parquet
            data: data.parquet
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.DatasetDict`` from a directory of files:

        >>> from datasets import Dataset, DatasetDict
        >>> from kedro_datasets.huggingface.parquet_dataset import (
        ...     ParquetDataset,
        ... )
        >>>
        >>> dataset_dict = DatasetDict({
        ...     "labels": Dataset.from_dict({"col1": [1, 2], "col2": ["a", "b"]}),
        ...     "data": Dataset.from_dict({"col1": [3, 4], "col2": ["c", "d"]}),
        ... })
        >>> dataset = ParquetDataset(
        ...     path=tmp_path,
        ...     data_files={
        ...         "labels": "labels.parquet",
        ...         "data": "data.parquet",
        ...     },
        ... )
        >>> dataset.save(dataset_dict)
        >>> loaded = dataset.load()
    """

    BUILDER: ClassVar[str] = "parquet"
    EXTENSION: ClassVar[str] = ".parquet"
