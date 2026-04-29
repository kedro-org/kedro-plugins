from __future__ import annotations

from typing import ClassVar

from ._base import FilesystemDataset


class JSONDataset(FilesystemDataset):
    """``JSONDataset`` loads/saves Hugging Face ``Dataset`` and
    ``DatasetDict`` objects to/from JSON files.

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
          type: huggingface.JSONDataset
          path: data/01_raw/reviews.json
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.DatasetDict`` from a single file:

        >>> from kedro_datasets.huggingface.json_dataset import (
        ...     JSONDataset,
        ... )
        >>>
        >>> dataset = JSONDataset(path=tmp_path / "data.json")
        >>> loaded = dataset.load()
        >>> assert "train" in loaded

        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        to load a ``datasets.Dataset`` from a single file:

        ```yaml
        reviews:
          type: huggingface.JSONDataset
          path: data/01_raw/reviews.json
          load_args:
            split: train
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.Dataset`` from a single file:

        >>> from kedro_datasets.huggingface.json_dataset import (
        ...     JSONDataset,
        ... )
        >>>
        >>> dataset = JSONDataset(
        ...     path=tmp_path / "data.json",
        ...     load_args={"split": "train"},
        ... )
        >>> loaded = dataset.load()
        >>> assert type(loaded.shape) is tuple  # No "train" key.

        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)
        to load a ``datasets.DatasetDict`` from a directory of files:

        ```yaml
        reviews:
          type: huggingface.JSONDataset
          path: data/01_raw/reviews
          load_args:
            data_files:
              labels: labels.json
              data: data.json
        ```

        Using the
        [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)
        to load a ``datasets.DatasetDict`` from a directory of files:

        >>> from kedro_datasets.huggingface.json_dataset import (
        ...     JSONDataset,
        ... )
        >>>
        >>> dataset = JSONDataset(
        ...     path=tmp_path,
        ...     load_args={
        ...         "data_files": {
        ...             "labels": "labels.json",
        ...             "data": "data.json",
        ...         }
        ...     },
        ... )
        >>> loaded = dataset.load()
    """

    BUILDER: ClassVar[str] = "json"
    EXTENSION: ClassVar[str] = ".json"
