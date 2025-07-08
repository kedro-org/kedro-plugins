import pytest
from huggingface_hub import HfApi

from kedro_datasets.huggingface import HFDataset


@pytest.fixture
def dataset_name():
    return "yelp_review_full"


@pytest.fixture
def revision():
    return "main"


class TestHFDataset:
    def test_load_dataset_with_revision(self, dataset_name, revision, mocker):
        mocked_load_dataset = mocker.patch(
            "kedro_datasets.huggingface.hugging_face_dataset.load_dataset"
        )

        dataset = HFDataset(dataset_name=dataset_name, revision=revision)
        hf_ds = dataset.load()

        mocked_load_dataset.assert_called_once_with(dataset_name, revision=revision)
        assert hf_ds is mocked_load_dataset.return_value

    def test_list_datasets(self, mocker):
        expected_datasets = ["dataset_1", "dataset_2"]
        mocked_hf_list_datasets = mocker.patch.object(HfApi, "list_datasets")
        mocked_hf_list_datasets.return_value = expected_datasets

        datasets = HFDataset.list_datasets()

        assert datasets == expected_datasets

    def test_dataset_kwargs_combined(self, dataset_name, revision, mocker):
        mocked_load_dataset = mocker.patch(
            "kedro_datasets.huggingface.hugging_face_dataset.load_dataset"
        )

        dataset_kwargs = {"split": "train"}
        dataset = HFDataset(
            dataset_name=dataset_name,
            revision=revision,
            dataset_kwargs=dataset_kwargs,
        )
        hf_ds = dataset.load()

        mocked_load_dataset.assert_called_once_with(
            dataset_name, revision=revision, split="train"
        )
        assert hf_ds is mocked_load_dataset.return_value

    def test_missing_revision_raises(self, dataset_name):
        with pytest.raises(
            ValueError,
            match="kedro_datasets.huggingface.hugging_face_dataset.HFDataset requires `revision` to be set",
        ):
            HFDataset(dataset_name=dataset_name, revision=None).load()  # type: ignore
