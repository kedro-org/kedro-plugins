from __future__ import annotations

from typing import ClassVar

from datasets import Dataset, DatasetDict
from kedro.io.core import DatasetError

from ._base import FilesystemDataset


class HDF5Dataset(FilesystemDataset):
    """``HDF5Dataset`` loads Hugging Face ``Dataset`` and
    ``DatasetDict`` objects from
    `HDF5 <https://www.hdfgroup.org/solutions/hdf5/>`_ files.

    Saving is **not** supported because the ``datasets`` library does
    not provide a ``to_hdf5`` method.

    Examples:
        Using the
        [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/):

        ```yaml
        reviews:
          type: huggingface.HDF5Dataset
          path: data/01_raw/reviews.h5
        ```
    """

    BUILDER: ClassVar[str] = "hdf5"
    EXTENSION: ClassVar[str] = ".h5"

    def _save_dataset(self, data: Dataset, save_path: str) -> None:
        msg = "Saving in hdf5 format is not supported by the Hugging Face datasets library."
        raise DatasetError(msg)

    def _save_dataset_dict(self, data: DatasetDict, save_path: str) -> None:
        msg = "Saving in hdf5 format is not supported by the Hugging Face datasets library."
        raise DatasetError(msg)
