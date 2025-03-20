import os
from pathlib import Path, PurePosixPath

import boto3
import pytest
import xarray as xr
from kedro.io.core import DatasetError, Version
from moto import mock_aws
from s3fs import S3FileSystem
from xarray.testing import assert_equal

from kedro_datasets_experimental.netcdf import VersionedNetCDFDataset

FILE_NAME = "test.nc"
MULTIFILE_NAME = "test*.nc"
BUCKET_NAME = "test_bucket"
AWS_CREDENTIALS = {"key": "FAKE_ACCESS_KEY", "secret": "FAKE_SECRET_KEY"}

# Pathlib cannot be used since it strips out the second slash from "s3://"
S3_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"


@pytest.fixture
def dummy_xr_dataset() -> xr.Dataset:
    """Sample xarray dataset for load/save testing."""
    ds = xr.DataArray(
        [0, 1, 2, 3], dims=["x"], coords={"x": [0, 1, 2, 3]}, name="data"
    ).to_dataset()
    return ds


@pytest.fixture
def filepath_netcdf(tmp_path):
    """Fixture for a netcdf filepath."""
    return (tmp_path / FILE_NAME).as_posix()


@pytest.fixture
def netcdf_dataset(filepath_netcdf, load_args, save_args, fs_args):
    """Fixture for a non-versioned NetCDF dataset."""
    return VersionedNetCDFDataset(
        filepath=filepath_netcdf,
        load_args=load_args,
        save_args=save_args,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_netcdf_dataset(filepath_netcdf, load_version, save_version):
    """Fixture for a versioned NetCDF dataset."""
    return VersionedNetCDFDataset(
        filepath=filepath_netcdf, version=Version(load_version, save_version)
    )


@pytest.fixture
def mocked_s3_bucket():
    """Create a bucket for testing to store a singular NetCDF file."""
    with mock_aws():
        conn = boto3.client(
            "s3",
            aws_access_key_id=AWS_CREDENTIALS["key"],
            aws_secret_access_key=AWS_CREDENTIALS["secret"],
        )
        conn.create_bucket(Bucket=BUCKET_NAME)
        yield conn


@pytest.fixture()
def s3fs_cleanup():
    # clear cache so we get a clean slate every time we instantiate a S3FileSystem
    yield
    S3FileSystem.cachable = False


class TestVersionedNetCDFDataset:
    """Test class for VersionedNetCDFDataset."""

    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.nc"
        ds = VersionedNetCDFDataset(filepath=filepath)
        ds_versioned = VersionedNetCDFDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "VersionedNetCDFDataset" in str(ds_versioned)
        assert "VersionedNetCDFDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        reloaded_ds = versioned_netcdf_dataset.load()
        assert_equal(dummy_xr_dataset, reloaded_ds)

    def test_no_versions(self, versioned_netcdf_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for VersionedNetCDFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_netcdf_dataset.load()

    def test_exists(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_netcdf_dataset.exists()
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        assert versioned_netcdf_dataset.exists()

    def test_prevent_overwrite(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Check the error when attempting to override the dataset if the
        corresponding netcdf file for a given save version already exists."""
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        pattern = (
            r"Save path \'.+\' for VersionedNetCDFDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_netcdf_dataset.save(dummy_xr_dataset)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self,
        versioned_netcdf_dataset,
        load_version,
        save_version,
        dummy_xr_dataset,
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for VersionedNetCDFDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_netcdf_dataset.save(dummy_xr_dataset)

    def test_http_filesystem_no_versioning(self):
        """Test that HTTP protocol does not support versioning."""
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            VersionedNetCDFDataset(
                filepath="https://example.com/test.nc", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, netcdf_dataset, versioned_netcdf_dataset, dummy_xr_dataset
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        netcdf_dataset.save(dummy_xr_dataset)
        assert netcdf_dataset.exists()
        assert netcdf_dataset._filepath == versioned_netcdf_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_netcdf_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_netcdf_dataset.save(dummy_xr_dataset)

        # Remove non-versioned dataset and try again
        Path(netcdf_dataset._filepath.as_posix()).unlink()
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        assert versioned_netcdf_dataset.exists()


if __name__ == "__main__":
    # This allows running just this test for debugging purposes
    pytest.main(["-vvs", __file__])
