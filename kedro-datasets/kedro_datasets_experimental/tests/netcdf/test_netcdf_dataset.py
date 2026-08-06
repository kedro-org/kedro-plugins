import os

import boto3
import pytest
import xarray as xr
from kedro.io.core import DatasetError, Version
from moto import mock_aws
from s3fs import S3FileSystem
from xarray.testing import assert_equal

from kedro_datasets_experimental.netcdf import NetCDFDataset

FILE_NAME = "test.nc"
MULTIFILE_NAME = "test*.nc"
BUCKET_NAME = "test_bucket"
MULTIFILE_BUCKET_NAME = "test_bucket_multi"
AWS_CREDENTIALS = {"key": "FAKE_ACCESS_KEY", "secret": "FAKE_SECRET_KEY"}

# Pathlib cannot be used since it strips out the second slash from "s3://"
S3_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"
S3_PATH_MULTIFILE = f"s3://{MULTIFILE_BUCKET_NAME}/{MULTIFILE_NAME}"


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


@pytest.fixture
def mocked_s3_bucket_multi():
    """Create a bucket for testing to store multiple NetCDF files."""
    with mock_aws():
        conn = boto3.client(
            "s3",
            aws_access_key_id=AWS_CREDENTIALS["key"],
            aws_secret_access_key=AWS_CREDENTIALS["secret"],
        )
        conn.create_bucket(Bucket=MULTIFILE_BUCKET_NAME)
        yield conn


def dummy_data() -> xr.Dataset:
    """Sample xarray dataset for load/save testing."""
    ds = xr.DataArray(
        [0, 1, 2, 3], dims=["x"], coords={"x": [0, 1, 2, 3]}, name="data"
    ).to_dataset()
    return ds


@pytest.fixture
def dummy_xr_dataset() -> xr.Dataset:
    """Expected result for load/save on a single NetCDF file."""
    return dummy_data()


@pytest.fixture
def dummy_xr_dataset_multi() -> xr.Dataset:
    """Expected concatenated result for load/save on multiple NetCDF files."""
    data = dummy_data()
    return xr.concat([data, data], dim="dummy")


@pytest.fixture
def mocked_s3_object(tmp_path, mocked_s3_bucket, dummy_xr_dataset: xr.Dataset):
    """Creates singular test NetCDF and adds it to mocked S3 bucket."""
    temporary_path = tmp_path / FILE_NAME
    dummy_xr_dataset.to_netcdf(str(temporary_path))

    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME, Key=FILE_NAME, Body=temporary_path.read_bytes()
    )
    return mocked_s3_bucket


@pytest.fixture
def mocked_s3_object_multi(
    tmp_path, mocked_s3_bucket_multi, dummy_xr_dataset: xr.Dataset
):
    """Creates multiple test NetCDFs and adds them to mocked S3 bucket."""

    def put_data(file_name: str):
        temporary_path = tmp_path / file_name
        dummy_xr_dataset.to_netcdf(str(temporary_path))
        mocked_s3_bucket_multi.put_object(
            Bucket=MULTIFILE_BUCKET_NAME,
            Key=file_name,
            Body=temporary_path.read_bytes(),
        )
        return mocked_s3_bucket_multi

    mocked_s3_bucket_multi = put_data("test1.nc")
    mocked_s3_bucket_multi = put_data("test2.nc")
    return mocked_s3_bucket_multi


@pytest.fixture
def s3_dataset(load_args, save_args, tmp_path):
    """Sample NetCDF dataset pointing to mocked S3 bucket with single NetCDF file."""
    return NetCDFDataset(
        filepath=S3_PATH,
        temppath=tmp_path,
        credentials=AWS_CREDENTIALS,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def s3_dataset_multi(save_args, tmp_path):
    """Sample NetCDF dataset pointing to mocked S3 bucket with multiple NetCDF files."""
    return NetCDFDataset(
        filepath=S3_PATH_MULTIFILE,
        temppath=tmp_path,
        credentials=AWS_CREDENTIALS,
        load_args={"concat_dim": "dummy", "combine": "nested"},
        save_args=save_args,
    )


@pytest.fixture
def versioned_netcdf_dataset(tmp_path, load_version, save_version):
    return NetCDFDataset(
        filepath=(tmp_path / FILE_NAME).as_posix(),
        version=Version(load_version, save_version),
    )


@pytest.fixture
def versioned_s3_dataset(tmp_path, load_version, save_version):
    return NetCDFDataset(
        filepath=S3_PATH,
        temppath=tmp_path,
        credentials=AWS_CREDENTIALS,
        version=Version(load_version, save_version),
    )


@pytest.fixture()
def s3fs_cleanup():
    # clear cache so we get a clean slate every time we instantiate a S3FileSystem
    yield
    S3FileSystem.cachable = False


@pytest.mark.usefixtures("s3fs_cleanup")
class TestNetCDFDataset:
    os.environ["AWS_ACCESS_KEY_ID"] = "FAKE_ACCESS_KEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "FAKE_SECRET_KEY"

    def test_temppath_error_raised(self):
        """Test that error is raised if S3 NetCDF file referenced without a temporary
        path."""
        pattern = "Need to set temppath in catalog"
        with pytest.raises(ValueError, match=pattern):
            NetCDFDataset(
                filepath=S3_PATH,
                temppath=None,
            )

    @pytest.mark.parametrize("bad_credentials", [{"key": None, "secret": None}])
    def test_empty_credentials_load(self, bad_credentials, tmp_path):
        """Test that error is raised if there are no AWS credentials."""
        netcdf_dataset = NetCDFDataset(
            filepath=S3_PATH, temppath=tmp_path, credentials=bad_credentials
        )
        pattern = r"Failed while loading data from dataset kedro_datasets_experimental.netcdf.netcdf_dataset.NetCDFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            netcdf_dataset.load()

    @pytest.mark.xfail(reason="Pending rewrite with new s3fs version")
    def test_pass_credentials(self, mocker, tmp_path):
        """Test that AWS credentials are passed successfully into boto3
        client instantiation on creating S3 connection."""
        client_mock = mocker.patch("botocore.session.Session.create_client")
        s3_dataset = NetCDFDataset(
            filepath=S3_PATH, temppath=tmp_path, credentials=AWS_CREDENTIALS
        )
        pattern = r"Failed while loading data from dataset kedro_datasets_experimental.netcdf.netcdf_dataset.NetCDFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            s3_dataset.load()

        assert client_mock.call_count == 1
        args, kwargs = client_mock.call_args_list[0]
        assert args == ("s3",)
        assert kwargs["aws_access_key_id"] == AWS_CREDENTIALS["key"]
        assert kwargs["aws_secret_access_key"] == AWS_CREDENTIALS["secret"]

    @pytest.mark.skip(reason="S3 tests that load datasets don't work properly")
    def test_save_data_single(self, s3_dataset, dummy_xr_dataset, mocked_s3_bucket):
        """Test saving a single NetCDF file to S3."""
        s3_dataset.save(dummy_xr_dataset)
        loaded_data = s3_dataset.load()
        assert_equal(loaded_data, dummy_xr_dataset)

    def test_save_data_multi_error(self, s3_dataset_multi, dummy_xr_dataset_multi):
        """Test that error is raised when trying to save to a NetCDF destination with
        a glob pattern."""
        pattern = r"Globbed multifile datasets with '*'"
        with pytest.raises(DatasetError, match=pattern):
            s3_dataset_multi.save(dummy_xr_dataset)

    @pytest.mark.skip(reason="S3 tests that load datasets don't work properly")
    def test_load_data_single(self, s3_dataset, dummy_xr_dataset, mocked_s3_object):
        """Test loading a single NetCDF file from S3."""
        loaded_data = s3_dataset.load()
        assert_equal(loaded_data, dummy_xr_dataset)

    @pytest.mark.skip(reason="S3 tests that load datasets don't work properly")
    def test_load_data_multi(
        self, s3_dataset_multi, dummy_xr_dataset_multi, mocked_s3_object_multi
    ):
        """Test loading multiple NetCDF files from S3."""
        loaded_data = s3_dataset_multi.load()
        assert_equal(loaded_data, dummy_xr_dataset_multi)

    @pytest.mark.skip(reason="Skipping for now, need to investigate")
    def test_exists(self, s3_dataset, dummy_xr_dataset, mocked_s3_bucket):
        """Test `exists` method invocation for both existing and nonexistent single
        NetCDF file."""
        assert not s3_dataset.exists()
        s3_dataset.save(dummy_xr_dataset)
        assert s3_dataset.exists()

    @pytest.mark.skip(reason="Skipping for now, need to investigate")
    @pytest.mark.usefixtures("mocked_s3_object_multi")
    def test_exists_multi_remote(self, s3_dataset_multi):
        """Test `exists` method invocation works for multifile glob pattern on S3."""
        assert s3_dataset_multi.exists()

    def test_exists_multi_locally(self, tmp_path, dummy_xr_dataset):
        """Test `exists` method invocation for both existing and nonexistent set of
        multiple local NetCDF files."""
        dataset = NetCDFDataset(filepath=str(tmp_path / MULTIFILE_NAME))
        assert not dataset.exists()
        NetCDFDataset(filepath=str(tmp_path / "test1.nc")).save(dummy_xr_dataset)
        NetCDFDataset(filepath=str(tmp_path / "test2.nc")).save(dummy_xr_dataset)
        assert dataset.exists()

    @pytest.mark.parametrize(
        "save_args, load_args",
        [
            ({"engine": "netcdf4"}, {"engine": "netcdf4"}),
            ({"engine": "scipy"}, {"engine": "scipy"}),
            ({"engine": "h5netcdf"}, {"engine": "h5netcdf"}),
        ],
        indirect=True,
    )
    def test_save_load_locally(self, tmp_path, dummy_xr_dataset, save_args, load_args):
        """Test loading and saving the a NetCDF file locally."""
        file_path = str(tmp_path / FILE_NAME)
        dataset = NetCDFDataset(filepath=file_path)

        assert not dataset.exists()
        dataset.save(dummy_xr_dataset)
        assert dataset.exists()
        loaded_data = dataset.load()
        dummy_xr_dataset.equals(loaded_data)

    def test_pathlike_filepath(self, tmp_path, dummy_xr_dataset):
        """Test that pathlib paths are accepted as filepaths."""
        dataset = NetCDFDataset(filepath=tmp_path / FILE_NAME)

        dataset.save(dummy_xr_dataset)

        assert_equal(dataset.load(), dummy_xr_dataset)

    def test_load_locally_multi(
        self, tmp_path, dummy_xr_dataset, dummy_xr_dataset_multi
    ):
        """Test loading multiple NetCDF files locally."""
        file_path = str(tmp_path / MULTIFILE_NAME)
        dataset = NetCDFDataset(
            filepath=file_path, load_args={"concat_dim": "dummy", "combine": "nested"}
        )

        assert not dataset.exists()
        NetCDFDataset(filepath=str(tmp_path / "test1.nc")).save(dummy_xr_dataset)
        NetCDFDataset(filepath=str(tmp_path / "test2.nc")).save(dummy_xr_dataset)
        assert dataset.exists()
        loaded_data = dataset.load()
        dummy_xr_dataset_multi.equals(loaded_data.compute())

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, s3_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert s3_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, s3_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert s3_dataset._save_args[key] == value

        for key, value in s3_dataset.DEFAULT_SAVE_ARGS.items():
            assert s3_dataset._save_args[key] == value


class TestNetCDFDatasetVersioned:
    def test_version_str_repr(self, tmp_path, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = FILE_NAME
        ds = NetCDFDataset(filepath=filepath)
        ds_versioned = NetCDFDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "NetCDFDataset" in str(ds_versioned)
        assert "NetCDFDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)

    def test_save_and_load(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        reloaded = versioned_netcdf_dataset.load()
        assert_equal(reloaded, dummy_xr_dataset)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    def test_save_and_load_remote(self, versioned_s3_dataset, dummy_xr_dataset, mocker):
        """Test saving and loading a versioned NetCDF file on S3."""
        mocker.patch.object(
            versioned_s3_dataset, "_exists_function", return_value=False
        )
        mocker.patch.object(versioned_s3_dataset._fs, "exists", return_value=True)
        mocker.patch.object(versioned_s3_dataset._fs, "put_file")
        mocker.patch.object(versioned_s3_dataset._fs, "get")
        mocker.patch.object(versioned_s3_dataset._fs, "invalidate_cache")

        versioned_s3_dataset.save(dummy_xr_dataset)

        assert versioned_s3_dataset.exists()
        assert_equal(versioned_s3_dataset.load(), dummy_xr_dataset)

        expected_path = "test_bucket/test.nc/2019-01-01T23.59.59.999Z/test.nc"
        versioned_s3_dataset._fs.put_file.assert_called_once()
        assert versioned_s3_dataset._fs.put_file.call_args.args[1] == expected_path
        versioned_s3_dataset._fs.get.assert_called_once()
        assert versioned_s3_dataset._fs.get.call_args.args[0] == expected_path

    def test_no_versions(self, versioned_netcdf_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for kedro_datasets_experimental.netcdf.netcdf_dataset.NetCDFDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_netcdf_dataset.load()

    def test_exists(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_netcdf_dataset.exists()
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        assert versioned_netcdf_dataset.exists()

    def test_prevent_overwrite(self, versioned_netcdf_dataset, dummy_xr_dataset):
        """Check the error when attempting to override the dataset if the
        corresponding NetCDF file for a given save version already exists."""
        versioned_netcdf_dataset.save(dummy_xr_dataset)
        pattern = (
            r"Save path \'.+\' for "
            r"kedro_datasets_experimental.netcdf.netcdf_dataset.NetCDFDataset"
            r"\(.+\) must not exist if versioning is enabled\."
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
        self, versioned_netcdf_dataset, load_version, save_version, dummy_xr_dataset
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for "
            rf"kedro_datasets_experimental.netcdf.netcdf_dataset.NetCDFDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_netcdf_dataset.save(dummy_xr_dataset)

    def test_multifile_no_versioning(self, tmp_path):
        pattern = "Versioning is not supported for globbed multifile NetCDF datasets."

        with pytest.raises(DatasetError, match=pattern):
            NetCDFDataset(
                filepath=(tmp_path / MULTIFILE_NAME).as_posix(),
                version=Version(None, None),
            )
