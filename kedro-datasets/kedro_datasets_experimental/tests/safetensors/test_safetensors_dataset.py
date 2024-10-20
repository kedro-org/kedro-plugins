import pytest
import torch

from kedro_datasets_experimental.safetensors import SafetensorsDataset


@pytest.fixture
def filepath(tmp_path):
    return (tmp_path / "test.safetensors").as_posix()


@pytest.fixture(params=["torch"])
def backend(request):
    return request.param


@pytest.fixture
def safetensors_dataset(filepath, backend, fs_args):
    return SafetensorsDataset(
        filepath=filepath,
        backend=backend,
        fs_args=fs_args,
    )

@pytest.fixture
def dummy_data():
    return {"embeddings": torch.zeros((10, 100))}


class TestSafetensorsDataset:
    @pytest.mark.parametrize(
        "backend",
        [
            "torch",
        ],
        indirect=True,
    )
    def test_save_and_load(self, safetensors_dataset, dummy_data):
        """Test saving and reloading the dataset."""
        safetensors_dataset.save(dummy_data)
        reloaded = safetensors_dataset.load()

        if safetensors_dataset._backend == "torch":
            assert torch.equal(dummy_data["embeddings"], reloaded["embeddings"])

        assert safetensors_dataset._fs_open_args_load == {}
        assert safetensors_dataset._fs_open_args_save == {"mode": "wb"}