import os
import sys
from pathlib import Path, PurePosixPath
from time import sleep

import boto3
import pandas as pd
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DataSetError
from kedro.io.core import PROTOCOL_DELIMITER, Version, generate_timestamp
from moto import mock_s3
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.pandas import CSVDataSet

BUCKET_NAME = "test_bucket"
FILE_NAME = "test.csv"


@pytest.fixture
def filepath_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


@pytest.fixture
def csv_data_set(filepath_csv, load_args, save_args, fs_args):
    return CSVDataSet(
        filepath=filepath_csv, load_args=load_args, save_args=save_args, fs_args=fs_args
    )


@pytest.fixture
def versioned_csv_data_set(filepath_csv, load_version, save_version):
    return CSVDataSet(
        filepath=filepath_csv, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_dataframe():
    return pd.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def partitioned_data_pandas():
    return {
        f"p{counter:02d}/data.csv": pd.DataFrame(
            {"part": counter, "col": list(range(counter + 1))}
        )
        for counter in range(5)
    }


@pytest.fixture
def mocked_s3_bucket():
    """Create a bucket for testing using moto."""
    with mock_s3():
        conn = boto3.client(
            "s3",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture
def mocked_dataframe():
    df = pd.DataFrame([{"dummy": "dummy"}])
    return df


@pytest.fixture
def mocked_csv_in_s3(mocked_s3_bucket, mocked_dataframe):
    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        Body=mocked_dataframe.to_csv(index=False),
    )
    return f"s3://{BUCKET_NAME}/{FILE_NAME}"


class TestCSVDataSet:
    def test_save_and_load(self, csv_data_set, dummy_dataframe):
        """Test saving and reloading the data set."""
        csv_data_set.save(dummy_dataframe)
        reloaded = csv_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded)

    def test_exists(self, csv_data_set, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not csv_data_set.exists()
        csv_data_set.save(dummy_dataframe)
        assert csv_data_set.exists()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, csv_data_set, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert csv_data_set._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, csv_data_set, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert csv_data_set._save_args[key] == value

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

        ds = CSVDataSet(filepath=filepath, load_args=load_args, save_args=save_args)

        records = [r for r in caplog.records if r.levelname == "WARNING"]
        expected_log_message = (
            f"Dropping 'storage_options' for {filepath}, "
            f"please specify them under 'fs_args' or 'credentials'."
        )
        assert records[0].getMessage() == expected_log_message
        assert "storage_options" not in ds._save_args
        assert "storage_options" not in ds._load_args

    @pytest.mark.parametrize(
        "nrows,expected",
        [
            (
                0,
                {
                    "index": [],
                    "columns": ["col1", "col2", "col3"],
                    "data": [],
                },
            ),
            (
                1,
                {
                    "index": [0],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5]],
                },
            ),
            (
                None,
                {
                    "index": [0, 1],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5], [2, 5, 6]],
                },
            ),
            (
                10,
                {
                    "index": [0, 1],
                    "columns": ["col1", "col2", "col3"],
                    "data": [[1, 4, 5], [2, 5, 6]],
                },
            ),
        ],
    )
    def test_preview(self, csv_data_set, dummy_dataframe, nrows, expected):
        """Test _preview returns the correct data structure."""
        csv_data_set.save(dummy_dataframe)
        previewed = csv_data_set._preview(nrows=nrows)
        assert previewed == expected

    def test_load_missing_file(self, csv_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set CSVDataSet\(.*\)"
        with pytest.raises(DataSetError, match=pattern):
            csv_data_set.load()

    @pytest.mark.parametrize(
        "filepath,instance_type,credentials",
        [
            ("s3://bucket/file.csv", S3FileSystem, {}),
            ("file:///tmp/test.csv", LocalFileSystem, {}),
            ("/tmp/test.csv", LocalFileSystem, {}),
            ("gcs://bucket/file.csv", GCSFileSystem, {}),
            ("https://example.com/file.csv", HTTPFileSystem, {}),
            (
                "abfs://bucket/file.csv",
                AzureBlobFileSystem,
                {"account_name": "test", "account_key": "test"},
            ),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, credentials):
        data_set = CSVDataSet(filepath=filepath, credentials=credentials)
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.csv"
        data_set = CSVDataSet(filepath=filepath)
        assert data_set._version_cache.currsize == 0  # no cache if unversioned
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
        assert data_set._version_cache.currsize == 0


class TestCSVDataSetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.csv"
        ds = CSVDataSet(filepath=filepath)
        ds_versioned = CSVDataSet(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "CSVDataSet" in str(ds_versioned)
        assert "CSVDataSet" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        # Default save_args
        assert "save_args={'index': False}" in str(ds)
        assert "save_args={'index': False}" in str(ds_versioned)

    def test_save_and_load(self, versioned_csv_data_set, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_csv_data_set.save(dummy_dataframe)
        reloaded_df = versioned_csv_data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_multiple_loads(
        self, versioned_csv_data_set, dummy_dataframe, filepath_csv
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_csv_data_set.save(dummy_dataframe)
        versioned_csv_data_set.load()
        v1 = versioned_csv_data_set.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        CSVDataSet(filepath=filepath_csv, version=Version(v_new, v_new)).save(
            dummy_dataframe
        )

        versioned_csv_data_set.load()
        v2 = versioned_csv_data_set.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = CSVDataSet(filepath=filepath_csv, version=Version(None, None))
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_csv):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = CSVDataSet(filepath=filepath_csv, version=Version(None, None))

        # first save
        ds_versioned.save(dummy_dataframe)
        first_save_version = ds_versioned.resolve_save_version()
        first_load_version = ds_versioned.resolve_load_version()
        assert first_load_version == first_save_version

        # second save
        sleep(0.5)
        ds_versioned.save(dummy_dataframe)
        second_save_version = ds_versioned.resolve_save_version()
        second_load_version = ds_versioned.resolve_load_version()
        assert second_load_version == second_save_version
        assert second_load_version > first_load_version

        # another dataset
        ds_new = CSVDataSet(filepath=filepath_csv, version=Version(None, None))
        assert ds_new.resolve_load_version() == second_load_version

    def test_release_instance_cache(self, dummy_dataframe, filepath_csv):
        """Test that cache invalidation does not affect other instances"""
        ds_a = CSVDataSet(filepath=filepath_csv, version=Version(None, None))
        assert ds_a._version_cache.currsize == 0
        ds_a.save(dummy_dataframe)  # create a version
        assert ds_a._version_cache.currsize == 2

        ds_b = CSVDataSet(filepath=filepath_csv, version=Version(None, None))
        assert ds_b._version_cache.currsize == 0
        ds_b.resolve_save_version()
        assert ds_b._version_cache.currsize == 1
        ds_b.resolve_load_version()
        assert ds_b._version_cache.currsize == 2

        ds_a.release()

        # dataset A cache is cleared
        assert ds_a._version_cache.currsize == 0

        # dataset B cache is unaffected
        assert ds_b._version_cache.currsize == 2

    def test_no_versions(self, versioned_csv_data_set):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for CSVDataSet\(.+\)"
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.load()

    def test_exists(self, versioned_csv_data_set, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_csv_data_set.exists()
        versioned_csv_data_set.save(dummy_dataframe)
        assert versioned_csv_data_set.exists()

    def test_prevent_overwrite(self, versioned_csv_data_set, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding CSV file for a given save version already exists."""
        versioned_csv_data_set.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for CSVDataSet\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_csv_data_set, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for CSVDataSet\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = r"HTTP\(s\) DataSet doesn't support versioning\."

        with pytest.raises(DataSetError, match=pattern):
            CSVDataSet(
                filepath="https://example.com/file.csv", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, csv_data_set, versioned_csv_data_set, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        csv_data_set.save(dummy_dataframe)
        assert csv_data_set.exists()
        assert csv_data_set._filepath == versioned_csv_data_set._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_csv_data_set._filepath.parent.as_posix()})"
        )
        with pytest.raises(DataSetError, match=pattern):
            versioned_csv_data_set.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(csv_data_set._filepath.as_posix()).unlink()
        versioned_csv_data_set.save(dummy_dataframe)
        assert versioned_csv_data_set.exists()


class TestCSVDataSetS3:
    os.environ["AWS_ACCESS_KEY_ID"] = "FAKE_ACCESS_KEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKE_SECRET_KEY"

    def test_load_and_confirm(self, mocker, mocked_csv_in_s3, mocked_dataframe):
        """Test the standard flow for loading, confirming and reloading a
        IncrementalDataSet in S3

        Unmodified Test fails in Python >= 3.10 if executed after test_protocol_usage
        (any implementation using S3FileSystem). Likely to be a bug with moto (tested
        with moto==4.0.8, moto==3.0.4) -- see #67
        """
        df = CSVDataSet(mocked_csv_in_s3)
        assert df._protocol == "s3"
        # if Python >= 3.10, modify test procedure (see #67)
        if sys.version_info[1] >= 10:
            read_patch = mocker.patch("pandas.read_csv", return_value=mocked_dataframe)
            df.load()
            read_patch.assert_called_once_with(mocked_csv_in_s3, storage_options={})
        else:
            loaded = df.load()
            assert_frame_equal(loaded, mocked_dataframe)
