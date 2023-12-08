from __future__ import annotations

from typing import Any

from datasets import load_dataset
from huggingface_hub import HfApi
from kedro.io import AbstractVersionedDataset


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

       >>> from kedro_datasets.huggingface import HFDataset
       >>> dataset = HFDataset(dataset_name="yelp_review_full")
       >>> yelp_review_full = dataset.load()
       >>> assert "train" in yelp_review_full
       >>> assert "test" in yelp_review_full
       >>> assert len(yelp_review_full["train"]) == 650000

    """

    def __init__(self, *, dataset_name: str):
        self.dataset_name = dataset_name

    def _load(self):
        return load_dataset(self.dataset_name)

    def _save(self):
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
