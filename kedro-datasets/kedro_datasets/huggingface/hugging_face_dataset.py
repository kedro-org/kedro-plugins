from __future__ import annotations

from typing import Any

from datasets import load_dataset
from huggingface_hub import HfApi
from kedro.io import AbstractDataset


class HFDataset(AbstractDataset):
    """``HFDataset`` loads Hugging Face datasets
    using the `datasets <https://pypi.org/project/datasets>`_ library,
    and requires a `revision` (e.g., tag, version, or commit hash) to ensure
    secure and reproducible dataset loading.

    The `revision` argument is **mandatory** to avoid unsafe downloads and to comply
    with security checks like Bandit B615.

    Examples:
        Using the [YAML API](https://docs.kedro.org/en/stable/data/data_catalog_yaml_examples.html):

        ```yaml
        yelp_reviews:
          type: kedro_hf_datasets.HFDataset
          dataset_name: yelp_review_full
          revision: "main"
        ```

        Using the [Python API](https://docs.kedro.org/en/stable/data/advanced_data_catalog_usage.html):

        >>> from datasets.utils.logging import ERROR, disable_progress_bar, set_verbosity
        >>> from kedro_datasets.huggingface import HFDataset
        >>>
        >>> disable_progress_bar()  # for doctest to pass
        >>> set_verbosity(ERROR)  # for doctest to pass
        >>>
        >>> dataset = HFDataset(dataset_name="openai_humaneval", revision="main")
        >>> ds = dataset.load()
        >>> assert "test" in ds
        >>> assert len(ds["test"]) == 164
    """

    def __init__(
        self,
        *,
        dataset_name: str,
        revision: str,
        dataset_kwargs: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.dataset_name = dataset_name
        self._dataset_kwargs = dataset_kwargs or {}
        if revision:
            self._dataset_kwargs["revision"] = revision
        else:
            raise ValueError(
                f"{str(self)} requires `revision` to be set for secure dataset loading."
            )
        self.metadata = metadata

    def load(self):
        return load_dataset(
            self.dataset_name,
            revision=self._dataset_kwargs["revision"],
            **{k: v for k, v in self._dataset_kwargs.items() if k != "revision"},
        )

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
