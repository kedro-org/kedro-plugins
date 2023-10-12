from pathlib import PurePosixPath

import polars as pl
import pytest
from fsspec.implementations.local import LocalFileSystem
from kedro.io import DatasetError
from kedro.io.core import PROTOCOL_DELIMITER
from polars.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.polars import PolarsDataSet


@pytest.fixture
def filepath_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


@pytest.fixture
def filepath_pq(tmp_path):
    return (tmp_path / "test.pq").as_posix()


@pytest.fixture
def csv_data_set(filepath_csv, load_args, save_args, fs_args):
    return PolarsDataSet(
        filepath=filepath_csv,
        file_format="csv",
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def pq_data_set(filepath_pq, load_args, save_args, fs_args):
    return PolarsDataSet(
        filepath=filepath_pq,
        file_format="parquet",
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def dummy_dataframe():
    return pl.DataFrame({"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]})


class TestPolarsDataSet:
    """Test class for PolarsDataSet tests"""

    def test_save_and_csv_load(self, tmp_path, filepath_csv, dummy_dataframe):
        """Test saving and reloading the data set."""
        data_set = PolarsDataSet(filepath=filepath_csv, file_format="csv")
        data_set.save(dummy_dataframe)
        reloaded = data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded.collect())

        files = [child.is_file() for child in tmp_path.iterdir()]
        assert all(files)
        assert len(files) == 1

    def test_save_and_parquet_load(self, tmp_path, filepath_pq, dummy_dataframe):
        """Test saving and reloading the data set."""
        data_set = PolarsDataSet(filepath=filepath_pq, file_format="parquet")
        data_set.save(dummy_dataframe)
        reloaded = data_set.load()
        assert_frame_equal(dummy_dataframe, reloaded.collect())

        files = [child.is_file() for child in tmp_path.iterdir()]
        assert all(files)
        assert len(files) == 1

    def test_exists(self, csv_data_set, dummy_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set.
        """
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

        ds = PolarsDataSet(
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

    def test_load_missing_file(self, csv_data_set):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set PolarsDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            csv_data_set.load()

    @pytest.mark.parametrize(
        "filepath,instance_type,credentials",
        [
            ("s3://bucket/file.csv", S3FileSystem, {}),
            ("file:///tmp/test.csv", LocalFileSystem, {}),
            ("/tmp/test.csv", LocalFileSystem, {}),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type, credentials):
        data_set = PolarsDataSet(
            filepath=filepath, file_format="csv", credentials=credentials
        )
        assert isinstance(data_set._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(data_set._filepath) == path
        assert isinstance(data_set._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.csv"
        data_set = PolarsDataSet(filepath=filepath, file_format="csv")
        assert data_set._version_cache.currsize == 0  # no cache if unversioned
        data_set.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)
        assert data_set._version_cache.currsize == 0
