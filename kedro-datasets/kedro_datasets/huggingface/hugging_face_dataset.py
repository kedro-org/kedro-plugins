from __future__ import annotations

from typing import Any

from datasets import load_dataset
from huggingface_hub import HfApi
from kedro.io import AbstractVersionedDataset as AbstractVersionedDataSet
from kedro.io import DatasetError as DataSetError


class HFDataset(AbstractVersionedDataSet):
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name

    def _load(self):
        return load_dataset(self.dataset_name)

    def _save(self):
        raise DataSetError("_save not implemented for HFDataset")

    def _describe(self) -> dict[str, Any]:
        api = HfApi()
        dataset_info = list(api.list_datasets(search=self.dataset_name))[0]
        return {
            "dataset_name": self.dataset_name,
            "dataset_tags": dataset_info.tags,
            "dataset_author": dataset_info.author,
        }

    @staticmethod
    def fetch_datasets():
        api = HfApi()
        return list(api.list_datasets())
