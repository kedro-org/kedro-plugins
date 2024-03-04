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
        dataset_kwargs: dict[Any] | None = None,
    ):
        self.dataset_name = dataset_name
        self._dataset_kwargs = dataset_kwargs or {}

    def _load(self):
        return load_dataset(self.dataset_name, **self._pipeline_kwargs)

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
