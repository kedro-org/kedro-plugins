from pathlib import Path, PurePosixPath

import geopandas as gpd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version, generate_timestamp
from pandas.testing import assert_frame_equal
from s3fs import S3FileSystem
from shapely.geometry import Point

from kedro_datasets.geopandas import ParquetDataset


@pytest.fixture(params=[None])
def load_version(request):
    return request.param


@pytest.fixture(params=[None])
def save_version(request):
    return request.param or generate_timestamp()


@pytest.fixture
def filepath(tmp_path):
    return (tmp_path / "test.parquet").as_posix()


@pytest.fixture(params=[None])
def load_args(request):
    return request.param


@pytest.fixture(params=[None])
def save_args(request):
    return request.param


@pytest.fixture
def dummy_dataframe():
    return gpd.GeoDataFrame(
        {"col1": [1, 2], "col2": [4, 5], "col3": [5, 6]},
        geometry=[Point(1, 1), Point(2, 2)],
    )


@pytest.fixture
def parquet_dataset(filepath, load_args, save_args, fs_args):
    return ParquetDataset(
        filepath=filepath, load_args=load_args, save_args=save_args, fs_args=fs_args
    )


@pytest.fixture
def versioned_parquet_dataset(filepath, load_version, save_version):
    return ParquetDataset(
        filepath=filepath, version=Version(load_version, save_version)
    )


class TestParquetDataset:
    def test_save_and_load(self, parquet_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one."""
        parquet_dataset.save(dummy_dataframe)
        reloaded_df = parquet_dataset.load()
        assert_frame_equal(reloaded_df, dummy_dataframe)
        assert parquet_dataset._fs_open_args_load == {}
        assert parquet_dataset._fs_open_args_save == {"mode": "wb"}

    @pytest.mark.parametrize("parquet_dataset", [{"index": False}], indirect=True)
    def test_load_missing_file(self, parquet_dataset):
        """Check the error while trying to load from missing source."""
        pattern = r"Failed while loading data from data set ParquetDataSet"
        with pytest.raises(DatasetError, match=pattern):
            parquet_dataset.load()

    def test_exists(self, parquet_dataset, dummy_dataframe):
        """Test `exists` method invocation for both cases."""
        assert not parquet_dataset.exists()
        parquet_dataset.save(dummy_dataframe)
        assert parquet_dataset.exists()

    @pytest.mark.parametrize("load_args", [{"crs": "init:4326"}, {"crs": "init:2154"}])
    def test_load_extra_params(self, parquet_dataset, load_args):
        """Test overriding default save args"""
        for k, v in load_args.items():
            assert parquet_dataset._load_args[k] == v

    @pytest.mark.parametrize(
        "save_args", [{"driver": "ESRI Shapefile"}, {"driver": "GPKG"}]
    )
    def test_save_extra_params(self, parquet_dataset, save_args):
        """Test overriding default save args"""
        for k, v in save_args.items():
            assert parquet_dataset._save_args[k] == v

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, parquet_dataset, fs_args):
        assert parquet_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert parquet_dataset._fs_open_args_save == {"mode": "wb"}

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/file.parquet", S3FileSystem),
            ("/tmp/test.parquet", LocalFileSystem),
            ("gcs://bucket/file.parquet", GCSFileSystem),
            ("file:///tmp/file.parquet", LocalFileSystem),
            ("https://example.com/file.parquet", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        parquet_dataset = ParquetDataset(filepath=path)
        assert isinstance(parquet_dataset._fs, instance_type)

        path = path.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(parquet_dataset._filepath) == path
        assert isinstance(parquet_dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.parquet"
        parquet_dataset = ParquetDataset(filepath=filepath)
        parquet_dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestParquetDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.parquet"
        ds = ParquetDataset(filepath=filepath)
        ds_versioned = ParquetDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "ParquetDataset" in str(ds_versioned)
        assert "ParquetDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_parquet_dataset, dummy_dataframe):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_parquet_dataset.save(dummy_dataframe)
        reloaded_df = versioned_parquet_dataset.load()
        assert_frame_equal(reloaded_df, dummy_dataframe)

    def test_no_versions(self, versioned_parquet_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for ParquetDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.load()

    def test_exists(self, versioned_parquet_dataset, dummy_dataframe):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_parquet_dataset.exists()
        versioned_parquet_dataset.save(dummy_dataframe)
        assert versioned_parquet_dataset.exists()

    def test_prevent_override(self, versioned_parquet_dataset, dummy_dataframe):
        """Check the error when attempt to override the same data set
        version."""
        versioned_parquet_dataset.save(dummy_dataframe)
        pattern = (
            r"Save path \'.+\' for ParquetDataset\(.+\) must not "
            r"exist if versioning is enabled"
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
            rf"'{load_version}' for ParquetDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_parquet_dataset.save(dummy_dataframe)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            ParquetDataset(
                filepath="https://example/file.parquet", version=Version(None, None)
            )

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
