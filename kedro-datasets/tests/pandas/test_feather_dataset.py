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

from kedro_datasets.pandas import FeatherDataSet


@pytest.fixture
def filepath_feather(tmp_path):
    return (tmp_path / "test.feather").as_posix()


@pytest.fixture
def feather_data_set(filepath_feather, load_args, fs_args):
    return FeatherDataSet(
        filepath=filepath_feather, load_args=load_args, fs_args=fs_args
    )


@pytest.fixture
def versioned_feather_data_set(filepath_feather, load_version, save_version):
    return FeatherDataSet(
        filepath=filepath_feather, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestFeatherDataSet:
    def test_save_and_load(self, feather_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        feather_data_set.save(dummy_dataframe)
        reloaded = feather_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded)

    def test_exists(self, feather_data_set, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not feather_data_set.exists()
        feather_data_set.save(dummy_dataframe)
        assert feather_data_set.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, feather_data_set, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert feather_data_set._load_args[key] == value

    @pytest.mark.parametrize(
        "load_args,save_args",
        [
            ({"storage_options": {"a": "b"}}, {}),
            ({}, {"storage_options": {"a": "b"}}),
            ({"storage_options": {"a": "b"}}, {"storage_options": {"x": "y"}}),
        ],
    )
    def test_storage_options_dropped(self, load_args, save_args, caplog, tmp_path):
        filepath = str(tmp_path / "test.csv")

        ds = FeatherDataSet(filepath=filepath, load_args=load_args, save_args=save_args)

        records = [r for r in caplog.records if r.levelname == "WARNING"]
        expected_log_message = (
            f"Dropping 'storage_options' for {filepath}, "
            f"please specify them under 'fs_args' or 'credentials'."
        )
        assert records[0].getMessage() == expected_log_message
        assert "storage_options" not in ds._save_args
        assert "storage_options" not in ds._load_args

    def test_load_missing_file(self, feather_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set FeatherDataSet\(.*\)"
        with pytest.raises(DataSetError, match=pattern):
            feather_data_set.load()

    @pytest.mark.parametrize(
        "filepath,instance_type,load_path",
        [
            ("s3://bucket/file.feather", S3FileSystem, "s3://bucket/file.feather"),
            ("file:///tmp/test.feather", LocalFileSystem, "/tmp/test.feather"),
            ("/tmp/test.feather", LocalFileSystem, "/tmp/test.feather"),
            ("gcs://bucket/file.feather", GCSFileSystem, "gcs://bucket/file.feather"),
            (
                "https://example.com/file.feather",
                HTTPFileSystem,
                "https://example.com/file.feather",
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, load_path, mocker):
        data_set = FeatherDataSet(filepath=filepath)
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

        mock_pandas_call = mocker.patch("pandas.read_feather")
        data_set.load()
        assert mock_pandas_call.call_count == 1
        assert mock_pandas_call.call_args_list[0][0][0] == load_path

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.feather"
        data_set = FeatherDataSet(filepath=filepath)
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestFeatherDataSetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.feather"
        ds = FeatherDataSet(filepath=filepath)
        ds_versioned = FeatherDataSet(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "FeatherDataSet" in str(ds_versioned)
        assert "FeatherDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_feather_data_set, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_feather_data_set.save(dummy_dataframe)
        reloaded_df = versioned_feather_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_no_versions(self, versioned_feather_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for FeatherDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_feather_data_set.load()

    def test_exists(self, versioned_feather_data_set, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_feather_data_set.exists()
        versioned_feather_data_set.save(dummy_dataframe)
        assert versioned_feather_data_set.exists()

    def test_prevent_overwrite(self, versioned_feather_data_set, dummy_dataframe):
        """Check the error when attempting to overwrite the data set if the
        corresponding feather file for a given save version already exists."""
        versioned_feather_data_set.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for FeatherDataSet\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_feather_data_set.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_feather_data_set, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for FeatherDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_feather_data_set.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = r"HTTP\(s\) DataSet doesn't support versioning\."

        with pytest.raises(DataSetError, match=pattern):
            FeatherDataSet(
                filepath="https://example.com/file.feather", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, feather_data_set, versioned_feather_data_set, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        feather_data_set.save(dummy_dataframe)
        assert feather_data_set.exists()
        assert feather_data_set._filepath == versioned_feather_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_feather_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_feather_data_set.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(feather_data_set._filepath.as_posix()).unlink()
        versioned_feather_data_set.save(dummy_dataframe)
        assert versioned_feather_data_set.exists()
