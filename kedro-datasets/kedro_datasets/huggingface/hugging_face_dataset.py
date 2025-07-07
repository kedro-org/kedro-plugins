from __future__ import annotations

from typing import Any

from datasets import load_dataset
from huggingface_hub import HfApi
from kedro.io import AbstractDataset


class HFDataset(AbstractDataset):
    """``HFDataset`` loads Hugging Face datasets
    using the `datasets <https://pypi.org/project/datasets>`_ library.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        yelp_reviews:
          type: kedro_hf_datasets.HFDataset
          dataset_name: yelp_review_full
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from datasets.utils.logging import ERROR, disable_progress_bar, set_verbosity
        >>> from kedro_datasets.huggingface import HFDataset
        >>>
        >>> disable_progress_bar()  # for doctest to pass
        >>> set_verbosity(ERROR)  # for doctest to pass
        >>>
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
        dataset_kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.dataset_name = dataset_name
        self._dataset_kwargs = dataset_kwargs or {}
        self.metadata = metadata

    def load(self):
        # TODO: Replace suppression with the solution from here: https://github.com/kedro-org/kedro-plugins/issues/1131
        return load_dataset(self.dataset_name, **self._dataset_kwargs)  # nosec

    def save(self):
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
