import pytest

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
