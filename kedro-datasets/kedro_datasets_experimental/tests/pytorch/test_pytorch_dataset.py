import pytest
import torch
import torch.nn.functional as F
from torch import nn

from kedro_datasets_experimental.pytorch import PyTorchDataset


# Define model
class TheModelClass(nn.Module):
    def __init__(self):
        super().__init__()
        self.conv1 = nn.Conv2d(3, 6, 5)
        self.pool = nn.MaxPool2d(2, 2)
        self.conv2 = nn.Conv2d(6, 16, 5)
        self.fc1 = nn.Linear(16 * 5 * 5, 120)
        self.fc2 = nn.Linear(120, 84)
        self.fc3 = nn.Linear(84, 10)

    def forward(self, x):
        x = self.pool(F.relu(self.conv1(x)))
        x = self.pool(F.relu(self.conv2(x)))
        x = x.view(-1, 16 * 5 * 5)
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))
        x = self.fc3(x)
        return x


@pytest.fixture
def filepath_model(tmp_path):
    return (tmp_path / "model.pt").as_posix()


@pytest.fixture
def pytorch_dataset(filepath_model, load_args, save_args, fs_args):
    return PyTorchDataset(
        filepath=filepath_model,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_pytorch_dataset(filepath_model, load_version, save_version):
    return PyTorchDataset(
        filepath=filepath_model, load_version=load_version, save_version=save_version
    )


@pytest.fixture
def dummy_model():
    return TheModelClass()


class TestPyTorchDataset:
    def test_save_and_load_dataset(self, pytorch_dataset, dummy_model, filepath_model):
        pytorch_dataset.save(dummy_model)
        model = TheModelClass()
        model.load_state_dict(pytorch_dataset.load())
        reloaded_state_dict = model.state_dict()
        dummy_state_dict = dummy_model.state_dict()
        for key, value in reloaded_state_dict.items():
            assert torch.equal(dummy_state_dict[key], value)
