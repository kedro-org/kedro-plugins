from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.pandas import HDFDataset

HDF_KEY = "data"


@pytest.fixture
def filepath_hdf(tmp_path):
    return (tmp_path / "test.h5").as_posix()


@pytest.fixture
def hdf_dataset(filepath_hdf, load_args, save_args, mocker, fs_args):
    HDFDataset._lock = mocker.MagicMock()
    return HDFDataset(
        filepath=filepath_hdf,
        key=HDF_KEY,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_hdf_dataset(filepath_hdf, load_version, save_version):
    return HDFDataset(
        filepath=filepath_hdf, key=HDF_KEY, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestHDFDataset:
    def test_save_and_load(self, hdf_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        hdf_dataset.save(dummy_dataframe)
        reloaded = hdf_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded)
        assert hdf_dataset._fs_open_args_load == {}
        assert hdf_dataset._fs_open_args_save == {"mode": "wb"}

    def test_exists(self, hdf_dataset, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not hdf_dataset.exists()
        hdf_dataset.save(dummy_dataframe)
        assert hdf_dataset.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, hdf_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert hdf_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, hdf_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert hdf_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, hdf_dataset, fs_args):
        assert hdf_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert hdf_dataset._fs_open_args_save == {"mode": "wb"}  # default unchanged

    def test_load_missing_file(self, hdf_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set HDFDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            hdf_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.h5", S3FileSystem),
            ("file:///tmp/test.h5", LocalFileSystem),
            ("/tmp/test.h5", LocalFileSystem),
            ("gcs://bucket/file.h5", GCSFileSystem),
            ("https://example.com/file.h5", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = HDFDataset(filepath=filepath, key=HDF_KEY)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.h5"
        dataset = HDFDataset(filepath=filepath, key=HDF_KEY)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_save_and_load_df_with_categorical_variables(self, hdf_dataset):
        """Test saving and reloading the data set with categorical variables."""
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": pd.Series(list("aab")).astype("category")}
        )
        hdf_dataset.save(df)
        reloaded = hdf_dataset.load()
        assert_frame_equal(df, reloaded)

    def test_thread_lock_usage(self, hdf_dataset, dummy_dataframe, mocker):
        """Test thread lock usage."""
        mocked_lock = HDFDataset._lock
        mocked_lock.assert_not_called()

        hdf_dataset.save(dummy_dataframe)
        calls = [
            mocker.call.__enter__(),
            mocker.call.__exit__(None, None, None),
        ]
        mocked_lock.assert_has_calls(calls)

        mocked_lock.reset_mock()
        hdf_dataset.load()
        mocked_lock.assert_has_calls(calls)


class TestHDFDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.h5"
        ds = HDFDataset(filepath=filepath, key=HDF_KEY)
        ds_versioned = HDFDataset(
            filepath=filepath, key=HDF_KEY, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "HDFDataset" in str(ds_versioned)
        assert "HDFDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        assert "key" in str(ds_versioned)
        assert "key" in str(ds)

    def test_save_and_load(self, versioned_hdf_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_hdf_dataset.save(dummy_dataframe)
        reloaded_df = versioned_hdf_dataset.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_no_versions(self, versioned_hdf_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for HDFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_hdf_dataset.load()

    def test_exists(self, versioned_hdf_dataset, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_hdf_dataset.exists()
        versioned_hdf_dataset.save(dummy_dataframe)
        assert versioned_hdf_dataset.exists()

    def test_prevent_overwrite(self, versioned_hdf_dataset, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding hdf file for a given save version already exists."""
        versioned_hdf_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for HDFDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_hdf_dataset.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_hdf_dataset, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for HDFDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_hdf_dataset.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            HDFDataset(
                filepath="https://example.com/file.h5",
                key=HDF_KEY,
                version=Version(None, None),
            )

    def test_versioning_existing_dataset(
        self, hdf_dataset, versioned_hdf_dataset, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        hdf_dataset.save(dummy_dataframe)
        assert hdf_dataset.exists()
        assert hdf_dataset._filepath == versioned_hdf_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_hdf_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_hdf_dataset.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(hdf_dataset._filepath.as_posix()).unlink()
        versioned_hdf_dataset.save(dummy_dataframe)
        assert versioned_hdf_dataset.exists()
