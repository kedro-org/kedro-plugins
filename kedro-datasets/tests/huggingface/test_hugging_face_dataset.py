import pytest
from huggingface_hub import HfApi

from kedro_datasets.huggingface import HFDataset


@pytest.fixture
def dataset_name():
    return "yelp_review_full"


class TestHFDataset:
    def test_simple_dataset_load(self, dataset_name, mocker):
        mocked_load_dataset = mocker.patch(
            "kedro_datasets.huggingface.hugging_face_dataset.load_dataset"
        )

        dataset = HFDataset(
            dataset_name=dataset_name,
        )
        hf_ds = dataset.load()

        mocked_load_dataset.assert_called_once_with(dataset_name)
        assert hf_ds is mocked_load_dataset.return_value

    def test_list_datasets(self, mocker):
        expected_datasets = ["dataset_1", "dataset_2"]
        mocked_hf_list_datasets = mocker.patch.object(HfApi, "list_datasets")
        mocked_hf_list_datasets.return_value = expected_datasets

        datasets = HFDataset.list_datasets()

        assert datasets == expected_datasets
