from __future__ import annotations

from typing import ClassVar

from datasets import Dataset, DatasetDict
from kedro.io.core import DatasetError

from ._base import FilesystemDataset


class LanceDataset(FilesystemDataset):
    """``LanceDataset`` loads Hugging Face ``Dataset`` and
    ``DatasetDict`` objects from `Lance <https://lancedb.github.io/lance/>`_
    files.

    Saving is **not** supported because the ``datasets`` library does
    not provide a ``to_lance`` method.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        reviews:
          type: huggingface.LanceDataset
          path: data/01_raw/reviews.lance
        ```
    """

    BUILDER: ClassVar[str] = "lance"
    EXTENSION: ClassVar[str] = ".lance"

    def _save_dataset(self, data: Dataset, save_path: str) -> None:
        msg = "Saving in lance format is not supported by the Hugging Face datasets library."
        raise DatasetError(msg)

    def _save_dataset_dict(self, data: DatasetDict, save_path: str) -> None:
        msg = "Saving in lance format is not supported by the Hugging Face datasets library."
        raise DatasetError(msg)
