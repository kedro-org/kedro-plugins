import pickle
from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DataSetError
from kedro.io.core import PROTOCOL_DELIMITER, Version
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.pickle import PickleDataSet


@pytest.fixture
def filepath_pickle(tmp_path):
    return (tmp_path / "test.pkl").as_posix()


@pytest.fixture(params=["pickle"])
def backend(request):
    return request.param


@pytest.fixture
def pickle_data_set(filepath_pickle, backend, load_args, save_args, fs_args):
    return PickleDataSet(
        filepath=filepath_pickle,
        backend=backend,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_pickle_data_set(filepath_pickle, load_version, save_version):
    return PickleDataSet(
        filepath=filepath_pickle, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestPickleDataSet:
    @pytest.mark.parametrize(
        "backend,load_args,save_args",
        [
            ("pickle", None, None),
            ("joblib", None, None),
            ("dill", None, None),
            ("compress_pickle", {"compression": "lz4"}, {"compression": "lz4"}),
        ],
        indirect=True,
    )
    def test_save_and_load(self, pickle_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        pickle_data_set.save(dummy_dataframe)
        reloaded = pickle_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded)
        assert pickle_data_set._fs_open_args_load == {}
        assert pickle_data_set._fs_open_args_save == {"mode": "wb"}

    def test_exists(self, pickle_data_set, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not pickle_data_set.exists()
        pickle_data_set.save(dummy_dataframe)
        assert pickle_data_set.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "errors": "strict"}], indirect=True
    )
    def test_load_extra_params(self, pickle_data_set, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert pickle_data_set._load_args[key] == value

    @pytest.mark.parametrize("save_args", [{"k1": "v1", "protocol": 2}], indirect=True)
    def test_save_extra_params(self, pickle_data_set, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert pickle_data_set._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, pickle_data_set, fs_args):
        assert pickle_data_set._fs_open_args_load == fs_args["open_args_load"]
        assert pickle_data_set._fs_open_args_save == {"mode": "wb"}  # default unchanged

    def test_load_missing_file(self, pickle_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set PickleDataSet\(.*\)"
        with pytest.raises(DataSetError, match=pattern):
            pickle_data_set.load()

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
        data_set = PickleDataSet(filepath=filepath)
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.pkl"
        data_set = PickleDataSet(filepath=filepath)
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_unserialisable_data(self, pickle_data_set, dummy_dataframe, mocker):
        mocker.patch("pickle.dump", side_effect=pickle.PickleError)
        pattern = r".+ was not serialised due to:.*"

        with pytest.raises(DataSetError, match=pattern):
            pickle_data_set.save(dummy_dataframe)

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
            PickleDataSet(filepath="test.pkl", backend="invalid")

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
            PickleDataSet(filepath="test.pkl", backend="fake.backend.does.not.exist")

    def test_copy(self, pickle_data_set):
        pickle_data_set_copy = pickle_data_set._copy()
        assert pickle_data_set_copy is not pickle_data_set
        assert pickle_data_set_copy._describe() == pickle_data_set._describe()


class TestPickleDataSetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.pkl"
        ds = PickleDataSet(filepath=filepath)
        ds_versioned = PickleDataSet(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "PickleDataSet" in str(ds_versioned)
        assert "PickleDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        assert "backend" in str(ds_versioned)
        assert "backend" in str(ds)

    def test_save_and_load(self, versioned_pickle_data_set, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_pickle_data_set.save(dummy_dataframe)
        reloaded_df = versioned_pickle_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_no_versions(self, versioned_pickle_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for PickleDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_pickle_data_set.load()

    def test_exists(self, versioned_pickle_data_set, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_pickle_data_set.exists()
        versioned_pickle_data_set.save(dummy_dataframe)
        assert versioned_pickle_data_set.exists()

    def test_prevent_overwrite(self, versioned_pickle_data_set, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding Pickle file for a given save version already exists."""
        versioned_pickle_data_set.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for PickleDataSet\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_pickle_data_set.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_pickle_data_set, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for PickleDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_pickle_data_set.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = r"HTTP\(s\) DataSet doesn't support versioning\."

        with pytest.raises(DataSetError, match=pattern):
            PickleDataSet(
                filepath="https://example.com/file.pkl", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, pickle_data_set, versioned_pickle_data_set, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        pickle_data_set.save(dummy_dataframe)
        assert pickle_data_set.exists()
        assert pickle_data_set._filepath == versioned_pickle_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_pickle_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_pickle_data_set.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(pickle_data_set._filepath.as_posix()).unlink()
        versioned_pickle_data_set.save(dummy_dataframe)
        assert versioned_pickle_data_set.exists()

    def test_copy(self, versioned_pickle_data_set):
        pickle_data_set_copy = versioned_pickle_data_set._copy()
        assert pickle_data_set_copy is not versioned_pickle_data_set
        assert pickle_data_set_copy._describe() == versioned_pickle_data_set._describe()
