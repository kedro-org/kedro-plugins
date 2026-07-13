import shutil
import tempfile

import numpy as np
import pandas as pd
import pytest
from darts import TimeSeries
from darts.models import RNNModel

from kedro_datasets_experimental.darts import DartsTorchModelDataset


@pytest.fixture
def tfm_kwargs():
    return {
        "pl_trainer_kwargs": {
            "accelerator": "cpu",
            "enable_progress_bar": False,
            "enable_model_summary": False,
        }
    }


@pytest.fixture
def tmpdir_checkpoints_for_module():
    """Sets up a temporary directory for checkpoints that will be deleted after the test module (script) finished."""
    temp_work_dir = tempfile.mkdtemp(prefix="darts")
    yield temp_work_dir
    shutil.rmtree(temp_work_dir)


@pytest.fixture
def filepath_model(tmp_path):
    return (tmp_path / "model.pt").as_posix()


@pytest.fixture
def model_class():
    return "RNNModel"


@pytest.fixture
def model_name():
    return "unittest-model-lstm"


@pytest.fixture
def darts_torch_dataset(filepath_model, model_class):
    return DartsTorchModelDataset(
        filepath=filepath_model,
        model_class=model_class,
        load_args={},
        save_args={},
    )


@pytest.fixture
def load_from_checkpoint_darts_torch_dataset(filepath_model, model_class, model_name, tmpdir_checkpoints_for_module):
    return DartsTorchModelDataset(
        filepath=filepath_model,
        model_class=model_class,
        load_args={"load_method": "load_from_checkpoint",
                   "model_name": model_name, "best": False,
                   "map_location": "cpu", "work_dir": tmpdir_checkpoints_for_module},
        save_args={"save_model": False},
    )


@pytest.fixture
def trained_darts_torch_model(filepath_model, model_class, tfm_kwargs, tmpdir_checkpoints_for_module, model_name):
    times = pd.date_range("20130101", "20130410")
    pd_series = pd.Series(range(100), index=times)
    series: TimeSeries = TimeSeries.from_series(pd_series)
    model = RNNModel(
        input_chunk_length=1,
        model="LSTM",
        n_epochs=1,
        model_name=model_name,
        work_dir=tmpdir_checkpoints_for_module,
        save_checkpoints=True,
        force_reset=True,
        **tfm_kwargs,
    )
    model.fit(series)
    return model


class TestDartsTorchModelDataset:
    def test_save_and_load_dataset(self, trained_darts_torch_model, darts_torch_dataset):
        darts_torch_dataset.save(trained_darts_torch_model)

        model_loaded = darts_torch_dataset.load()

        pred1 = trained_darts_torch_model.predict(n=6)
        pred2 = model_loaded.predict(n=6)

        # Two models with the same parameters should deterministically yield the same output
        np.testing.assert_array_equal(pred1.values(), pred2.values())

    def test_load_from_checkpoint_dataset(
            self,
            trained_darts_torch_model,
            tmpdir_checkpoints_for_module,
            load_from_checkpoint_darts_torch_dataset
    ):
        model_loaded = load_from_checkpoint_darts_torch_dataset.load()
        pred1 = trained_darts_torch_model.predict(n=6)
        pred2 = model_loaded.predict(n=6)

        # Two models with the same parameters should deterministically yield the same output
        np.testing.assert_array_equal(pred1.values(), pred2.values())
