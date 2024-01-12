"""Tests ``PickleDataset``."""
import importlib
import pickle

import numpy as np
import pandas as pd
import pytest
import redis
from kedro.io.core import DatasetError
from pandas.testing import assert_frame_equal

from kedro_datasets.redis import PickleDataset


@pytest.fixture(params=["pickle"])
def backend(request):
    return request.param


@pytest.fixture(params=["key"])
def key(request):
    return request.param


@pytest.fixture
def redis_args():
    return {
        "from_url_args": {"arg1": "1", "arg2": "2", "url": "redis://127.0.0.1:6379"}
    }


@pytest.fixture
def dummy_object():
    """Test data for saving."""
    return pd.DataFrame(np.random.random((3, 3)), columns=["a", "b", "c"])


@pytest.fixture
def serialised_dummy_object(backend, dummy_object, save_args):
    """Serialise test data."""
    imported_backend = importlib.import_module(backend)
    save_args = save_args or {}
    return imported_backend.dumps(dummy_object, **save_args)


@pytest.fixture
def pickle_dataset(mocker, key, backend, load_args, save_args, redis_args):
    mocker.patch(
        "redis.StrictRedis.from_url", return_value=redis.Redis.from_url("redis://")
    )
    return PickleDataset(
        key=key,
        backend=backend,
        load_args=load_args,
        save_args=save_args,
        redis_args=redis_args,
    )


class TestPickleDataset:
    @pytest.mark.parametrize(
        "key,backend,load_args,save_args",
        [
            ("a", "pickle", None, None),
            (1, "dill", None, None),
            (2, "cloudpickle", None, None),
            ("key", "compress_pickle", {"compression": "lz4"}, {"compression": "lz4"}),
        ],
        indirect=True,
    )
    def test_save_and_load(
        self,
        pickle_dataset,
        mocker,
        dummy_object,
        serialised_dummy_object,
        key,
    ):
        """Test saving and reloading the data set."""
        set_mocker = mocker.patch("redis.StrictRedis.set")
        get_mocker = mocker.patch(
            "redis.StrictRedis.get", return_value=serialised_dummy_object
        )
        pickle_dataset.save(dummy_object)
        mocker.patch("redis.StrictRedis.exists", return_value=True)
        loaded_dummy_object = pickle_dataset.load()
        set_mocker.assert_called_once_with(
            key,
            serialised_dummy_object,
        )
        get_mocker.assert_called_once_with(key)
        assert_frame_equal(loaded_dummy_object, dummy_object)

    def test_exists(self, mocker, pickle_dataset, dummy_object, key):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        mocker.patch("redis.StrictRedis.exists", return_value=False)
        assert not pickle_dataset.exists()
        mocker.patch("redis.StrictRedis.set")
        pickle_dataset.save(dummy_object)
        exists_mocker = mocker.patch("redis.StrictRedis.exists", return_value=True)
        assert pickle_dataset.exists()
        exists_mocker.assert_called_once_with(key)

    def test_exists_raises_error(self, pickle_dataset):
        """Check the error when trying to assert existence with no redis server."""
        pattern = r"The existence of key "
        with pytest.raises(DatasetError, match=pattern):
            pickle_dataset.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "errors": "strict"}], indirect=True
    )
    def test_load_extra_params(self, pickle_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert pickle_dataset._load_args[key] == value

    @pytest.mark.parametrize("save_args", [{"k1": "v1", "protocol": 2}], indirect=True)
    def test_save_extra_params(self, pickle_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert pickle_dataset._save_args[key] == value

    def test_redis_extra_args(self, pickle_dataset, redis_args):
        assert pickle_dataset._redis_from_url_args == redis_args["from_url_args"]
        assert pickle_dataset._redis_set_args == {}  # default unchanged

    def test_load_missing_key(self, mocker, pickle_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"The provided key "
        mocker.patch("redis.StrictRedis.exists", return_value=False)
        with pytest.raises(DatasetError, match=pattern):
            pickle_dataset.load()

    def test_unserialisable_data(self, pickle_dataset, dummy_object, mocker):
        mocker.patch("pickle.dumps", side_effect=pickle.PickleError)
        pattern = r".+ was not serialised due to:.*"

        with pytest.raises(DatasetError, match=pattern):
            pickle_dataset.save(dummy_object)

    def test_invalid_backend(self, mocker):
        pattern = (
            r"Selected backend 'invalid' should satisfy the pickle interface. "
            r"Missing one of 'loads' and 'dumps' on the backend."
        )
        mocker.patch(
            "kedro_datasets.pickle.pickle_dataset.importlib.import_module",
            return_value=object,
        )
        with pytest.raises(ValueError, match=pattern):
            PickleDataset(key="key", backend="invalid")

    def test_no_backend(self, mocker):
        pattern = (
            r"Selected backend 'fake.backend.does.not.exist' could not be imported. "
            r"Make sure it is installed and importable."
        )
        mocker.patch(
            "kedro_datasets.pickle.pickle_dataset.importlib.import_module",
            side_effect=ImportError,
        )
        with pytest.raises(ImportError, match=pattern):
            PickleDataset(key="key", backend="fake.backend.does.not.exist")
