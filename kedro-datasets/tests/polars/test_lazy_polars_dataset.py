import re
from pathlib import Path, PurePosixPath
from time import sleep

import boto3
import polars as pl
import pytest
from adlfs import AzureBlobFileSystem
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DatasetError, Version
from kedro.io.core import PROTOCOL_DELIMITER, generate_timestamp
from moto import mock_aws
from polars.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.polars import LazyPolarsDataset
from kedro_datasets.polars.lazy_polars_dataset import ACCEPTED_FILE_FORMATS

BUCKET_NAME = "test_bucket"
FILE_NAME = "test.csv"


@pytest.fixture
def filepath_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


@pytest.fixture
def filepath_pq(tmp_path):
    return (tmp_path / "test.pq").as_posix()


@pytest.fixture
def dummy_dataframe():
    return pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


@pytest.fixture
def csv_dataset(filepath_csv, load_args, save_args, fs_args):
    return LazyPolarsDataset(
        filepath=filepath_csv,
        file_format="csv",
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def parquet_dataset(filepath_pq, load_args, save_args, fs_args):
    return LazyPolarsDataset(
        filepath=filepath_pq,
        file_format="parquet",
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def parquet_dataset_ignore(filepath_pq):
    return LazyPolarsDataset(
        filepath=filepath_pq,
        file_format="parquet",
        load_args={"low_memory": True},
    )


@pytest.fixture
def versioned_parquet_dataset(
    filepath_pq,
    save_args,
    load_version,
    save_version,
):
    return LazyPolarsDataset(
        filepath=filepath_pq,
        file_format="parquet",
        version=Version(load_version, save_version),
        save_args={},
    )


@pytest.fixture
def mocked_s3_bucket():
    """Create a bucket for testing using moto."""
    with mock_aws():
        conn = boto3.client(
            "s3",
            aws_access_key_id="fake_access_key",
            aws_secret_access_key="fake_secret_key",
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture
def mocked_csv_in_s3(mocked_s3_bucket, dummy_dataframe):
    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME,
        Key=FILE_NAME,
        Body=dummy_dataframe.write_csv(),
    )
    return f"s3://{BUCKET_NAME}/{FILE_NAME}"


class TestLazyCSVDataset:
    """Test class for LazyPolarsDataset csv functionality"""

    def test_exists(self, csv_dataset, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set.
        """
        assert not csv_dataset.exists()
        csv_dataset.save(dummy_dataframe)
        assert csv_dataset.exists()

    def test_load(self, dummy_dataframe, csv_dataset, filepath_csv):
        dummy_dataframe.write_csv(filepath_csv)
        df = csv_dataset.load()
        assert df.collect().shape == (2, 3)

    def test_load_s3(self, dummy_dataframe, mocked_csv_in_s3):
        ds = LazyPolarsDataset(filepath=mocked_csv_in_s3, file_format="csv")

        assert ds._protocol == "s3"

        loaded_df = ds.load().collect()
        assert_frame_equal(loaded_df, dummy_dataframe)

    def test_save_and_load(self, csv_dataset, dummy_dataframe):
        csv_dataset.save(dummy_dataframe)
        reloaded_df = csv_dataset.load().collect()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_load_missing_file(self, csv_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set LazyPolarsDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            csv_dataset.load()

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, csv_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert csv_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, csv_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert csv_dataset._save_args[key] == value

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

        ds = LazyPolarsDataset(
            filepath=filepath,
            file_format="csv",
            load_args=load_args,
            save_args=save_args,
        )

        records = [r for r in caplog.records if r.levelname == "WARNING"]
        expected_log_message = (
            f"Dropping 'storage_options' for {filepath}, "
            f"please specify them under 'fs_args' or 'credentials'."
        )
        assert records[0].getMessage() == expected_log_message
        assert "storage_options" not in ds._save_args
        assert "storage_options" not in ds._load_args

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
        dataset = LazyPolarsDataset(
            filepath=filepath,
            file_format="csv",
            credentials=credentials,
        )
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.csv"
        dataset = LazyPolarsDataset(filepath=filepath, file_format="csv")
        assert dataset._version_cache.currsize == 0  # no cache if unversioned
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
        assert dataset._version_cache.currsize == 0


class TestLazyParquetDatasetVersioned:
    def test_load_args(self, parquet_dataset_ignore, dummy_dataframe, filepath_pq):
        dummy_dataframe.write_parquet(filepath_pq)
        df = parquet_dataset_ignore.load().collect()
        assert df.shape == (2, 3)

    def test_save_and_load(self, versioned_parquet_dataset, dummy_dataframe):
        """Test saving and reloading the data set."""
        versioned_parquet_dataset.save(dummy_dataframe.lazy())
        reloaded_df = versioned_parquet_dataset.load().collect()
        assert_frame_equal(dummy_dataframe, reloaded_df)

    def test_version_str_repr(self, filepath_pq, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        ds = LazyPolarsDataset(filepath=filepath_pq, file_format="parquet")
        ds_versioned = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(load_version, save_version),
        )
        assert filepath_pq in str(ds)
        assert filepath_pq in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "LazyPolarsDataset" in str(ds_versioned)
        assert "LazyPolarsDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_multiple_loads(
        self, versioned_parquet_dataset, dummy_dataframe, filepath_pq
    ):
        """Test that if a new version is created mid-run, by an
        external system, it won't be loaded in the current run."""
        versioned_parquet_dataset.save(dummy_dataframe)
        versioned_parquet_dataset.load()
        v1 = versioned_parquet_dataset.resolve_load_version()

        sleep(0.5)
        # force-drop a newer version into the same location
        v_new = generate_timestamp()
        LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(v_new, v_new),
        ).save(dummy_dataframe)

        versioned_parquet_dataset.load()
        v2 = versioned_parquet_dataset.resolve_load_version()

        assert v2 == v1  # v2 should not be v_new!
        ds_new = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(None, None),
        )
        assert (
            ds_new.resolve_load_version() == v_new
        )  # new version is discoverable by a new instance

    def test_multiple_saves(self, dummy_dataframe, filepath_pq):
        """Test multiple cycles of save followed by load for the same dataset"""
        ds_versioned = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(None, None),
        )

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
        ds_new = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(None, None),
        )
        assert ds_new.resolve_load_version() == second_load_version

    def test_release_instance_cache(self, dummy_dataframe, filepath_pq):
        """Test that cache invalidation does not affect other instances"""
        ds_a = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(None, None),
        )
        assert ds_a._version_cache.currsize == 0
        ds_a.save(dummy_dataframe)  # create a version
        assert ds_a._version_cache.currsize == 2

        ds_b = LazyPolarsDataset(
            filepath=filepath_pq,
            file_format="parquet",
            version=Version(None, None),
        )
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

    def test_no_versions(self, versioned_parquet_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for LazyPolarsDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.load()

    def test_prevent_overwrite(self, versioned_parquet_dataset, dummy_dataframe):
        """Check the error when attempting to override the data set if the
        corresponding Generic (parquet) file for a given save version already exists."""
        versioned_parquet_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for LazyPolarsDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.save(dummy_dataframe)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_parquet_dataset, load_version, save_version, dummy_dataframe
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for LazyPolarsDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_parquet_dataset.save(dummy_dataframe)

    def test_versioning_existing_dataset(
        self, parquet_dataset, versioned_parquet_dataset, dummy_dataframe
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        parquet_dataset.save(dummy_dataframe)
        assert parquet_dataset.exists()
        assert parquet_dataset._filepath == versioned_parquet_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_parquet_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.save(dummy_dataframe)

        # Remove non-versioned dataset and try again
        Path(parquet_dataset._filepath.as_posix()).unlink()
        versioned_parquet_dataset.save(dummy_dataframe)
        assert versioned_parquet_dataset.exists()


class TestBadLazyPolarsDataset:
    def test_bad_file_format_argument(self):
        pattern = (
            "'kedro' is not an accepted format "
            f"({ACCEPTED_FILE_FORMATS}) ensure that your 'file_format' parameter "
            "has been defined correctly as per the Polars API "
            "https://pola-rs.github.io/polars/py-polars/html/reference/io.html"
        )
        with pytest.raises(DatasetError, match=re.escape(pattern)):
            LazyPolarsDataset(filepath="test.kedro", file_format="kedro")
