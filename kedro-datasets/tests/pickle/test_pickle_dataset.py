import pickle
from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.pickle import PickleDataset


@pytest.fixture
def filepath_pickle(tmp_path):
    return (tmp_path / "test.pkl").as_posix()


@pytest.fixture(params=["pickle"])
def backend(request):
    return request.param


@pytest.fixture
def pickle_dataset(filepath_pickle, backend, load_args, save_args, fs_args):
    return PickleDataset(
        filepath=filepath_pickle,
        backend=backend,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_pickle_dataset(filepath_pickle, load_version, save_version):
    return PickleDataset(
        filepath=filepath_pickle, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestPickleDataset:
    @pytest.mark.parametrize(
        "backend,load_args,save_args",
        [
            ("pickle", None, None),
            ("joblib", None, None),
            ("dill", None, None),
            ("cloudpickle", None, None),
            ("compress_pickle", {"compression": "lz4"}, {"compression": "lz4"}),
        ],
        indirect=True,
    )
    def test_save_and_load(self, pickle_dataset, dummy_dataframe):
        """Test saving and reloading the dataset."""
        pickle_dataset.save(dummy_dataframe)
        reloaded = pickle_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded)
        assert pickle_dataset._fs_open_args_load == {}
        assert pickle_dataset._fs_open_args_save == {"mode": "wb"}

    def test_exists(self, pickle_dataset, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not pickle_dataset.exists()
        pickle_dataset.save(dummy_dataframe)
        assert pickle_dataset.exists()

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

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, pickle_dataset, fs_args):
        assert pickle_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert pickle_dataset._fs_open_args_save == {"mode": "wb"}  # default unchanged

    def test_load_missing_file(self, pickle_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset kedro_datasets.pickle.pickle_dataset.PickleDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            pickle_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.pkl", S3FileSystem),
            ("file:///tmp/test.pkl", LocalFileSystem),
            ("/tmp/test.pkl", LocalFileSystem),
            ("gcs://bucket/file.pkl", GCSFileSystem),
            ("https://example.com/file.pkl", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = PickleDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.pkl"
        dataset = PickleDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_unserialisable_data(self, pickle_dataset, dummy_dataframe, mocker):
        mocker.patch("pickle.dump", side_effect=pickle.PickleError)
        pattern = r".+ was not serialised due to:.*"

        with pytest.raises(DatasetError, match=pattern):
            pickle_dataset.save(dummy_dataframe)

    def test_invalid_backend(self, mocker):
        pattern = (
            r"Selected backend 'invalid' should satisfy the pickle interface. "
            r"Missing one of 'load' and 'dump' on the backend."
        )
        mocker.patch(
            "kedro_datasets.pickle.pickle_dataset.importlib.import_module",
            return_value=object,
        )
        with pytest.raises(ValueError, match=pattern):
            PickleDataset(filepath="test.pkl", backend="invalid")

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
            PickleDataset(filepath="test.pkl", backend="fake.backend.does.not.exist")

    def test_copy(self, pickle_dataset):
        pickle_dataset_copy = pickle_dataset._copy()
        assert pickle_dataset_copy is not pickle_dataset
        assert pickle_dataset_copy._describe() == pickle_dataset._describe()


class TestPickleDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.pkl"
        ds = PickleDataset(filepath=filepath)
        ds_versioned = PickleDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "PickleDataset" in str(ds_versioned)
        assert "PickleDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        assert "backend" in str(ds_versioned)
        assert "backend" in str(ds)

    def test_save_and_load(self, versioned_pickle_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_pickle_dataset.save(dummy_dataframe)
        reloaded_df = versioned_pickle_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_no_versions(self, versioned_pickle_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for kedro_datasets.pickle.pickle_dataset.PickleDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_pickle_dataset.load()

    def test_exists(self, versioned_pickle_dataset, dummy_dataframe):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_pickle_dataset.exists()
        versioned_pickle_dataset.save(dummy_dataframe)
        assert versioned_pickle_dataset.exists()

    def test_prevent_overwrite(self, versioned_pickle_dataset, dummy_dataframe):
        """Check the error when attempting to override the dataset if the
        corresponding Pickle file for a given save version already exists."""
        versioned_pickle_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.pickle.pickle_dataset.PickleDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_pickle_dataset.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_pickle_dataset, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for kedro_datasets.pickle.pickle_dataset.PickleDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_pickle_dataset.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            PickleDataset(
                filepath="https://example.com/file.pkl", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, pickle_dataset, versioned_pickle_dataset, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        pickle_dataset.save(dummy_dataframe)
        assert pickle_dataset.exists()
        assert pickle_dataset._filepath == versioned_pickle_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_pickle_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_pickle_dataset.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(pickle_dataset._filepath.as_posix()).unlink()
        versioned_pickle_dataset.save(dummy_dataframe)
        assert versioned_pickle_dataset.exists()

    def test_copy(self, versioned_pickle_dataset):
        pickle_dataset_copy = versioned_pickle_dataset._copy()
        assert pickle_dataset_copy is not versioned_pickle_dataset
        assert pickle_dataset_copy._describe() == versioned_pickle_dataset._describe()
