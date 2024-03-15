from __future__ import annotations

from typing import Any

from datasets import load_dataset, Dataset
from huggingface_hub import HfApi
from kedro.io import AbstractVersionedDataset

import logging


logger = logging.getLogger(__file__)


class HFDataset(AbstractVersionedDataset):
    """``HFDataset`` loads Hugging Face datasets
    using the `datasets <https://pypi.org/project/datasets>`_ library.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    .. code-block:: yaml

       yelp_reviews:
         type: kedro_hf_datasets.HFDataset
         dataset_name: yelp_review_full

    Example usage for the :doc:`Python API <kedro:data/advanced_data_catalog_usage>`:

    .. code-block:: pycon

       >>> from datasets.utils.logging import disable_progress_bar, set_verbosity, ERROR
       >>> disable_progress_bar()  # for doctest to pass
       >>> set_verbosity(ERROR)  # for doctest to pass
       >>> from kedro_datasets.huggingface import HFDataset
       >>> dataset = HFDataset(dataset_name="openai_humaneval")
       >>> ds = dataset.load()  # doctest: +ELLIPSIS
       Downloading and preparing dataset ...
       Dataset ...
       >>> assert "test" in ds
       >>> assert len(ds["test"]) == 164

    """

    def __init__(
        self,
        *,
        dataset_name: str,
        filepath: str = None,
        credentials: dict[Any] = None,
        save_to_disk: bool = True,
        save_to_hub: bool = False,
    ):
        self.dataset_name = dataset_name
        self.filepath = filepath
        self.credentials = credentials
        self.save_to_disk = save_to_disk
        self.save_to_hub = save_to_hub

    def _load(self) -> Dataset:
        try:
            ds = Dataset.load_from_disk(self.filepath)
        except FileNotFoundError:
            ds = load_dataset(self.dataset_name)
        return ds

    def _save(self, data: Dataset):
        if self.save_to_disk:
            logger.info("Saving to local disk.")
            data.save_to_disk(self._filepath)

        if self.save_to_hub:
            logger.info("Saving to HuggingFace Hub")

            if isinstance(self.credentials, dict):
                token = self.credentials.get('write')
            elif isinstance(self.credentials, str):
                token = self.credentials
            else:
                token = None

            data.push_to_hub(self._dataset_name, token=token)

    def _describe(self) -> dict[str, Any]:
        api = HfApi()
        dataset_info = list(api.list_datasets(search=self.dataset_name))[0]
        return {
            "dataset_name": self.dataset_name,
            "filepath": self.filepath,
            "save_to_disk": self.save_to_disk,
            "save_to_hub": self.save_to_hub,
            "dataset_tags": dataset_info.tags,
            "dataset_author": dataset_info.author,
        }

    @staticmethod
    def list_datasets():
        api = HfApi()
        return list(api.list_datasets())
