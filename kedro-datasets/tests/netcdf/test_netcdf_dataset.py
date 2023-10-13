import boto3
import pytest
import xarray as xr
from moto import mock_s3
from s3fs import S3FileSystem
from xarray.testing import assert_equal

from kedro_datasets._io import DatasetError
from kedro_datasets.netcdf import NetCDFDataSet

FILE_NAME = "test.nc"
BUCKET_NAME = "test_bucket"
AWS_CREDENTIALS = {"key": "FAKE_ACCESS_KEY", "secret": "FAKE_SECRET_KEY"}

# Pathlib cannot be used since it strips out the second slash from "s3://"
S3_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"


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
def dummy_xr_dataset() -> xr.Dataset:
    ds = xr.DataArray(
        [0, 1, 2, 3], dims=["x"], coords={"x": [0, 1, 2, 3]}, name="data"
    ).to_dataset()
    return ds


@pytest.fixture
def mocked_s3_object(tmp_path, mocked_s3_bucket, dummy_xr_dataset: xr.Dataset):
    """Creates test data and adds it to mocked S3 bucket."""
    temporary_path = tmp_path / FILE_NAME
    dummy_xr_dataset.to_netcdf(str(temporary_path))

    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME, Key=FILE_NAME, Body=temporary_path.read_bytes()
    )
    return mocked_s3_bucket


@pytest.fixture
def s3_dataset(load_args, save_args, tmp_path):
    return NetCDFDataSet(
        filepath=S3_PATH,
        temppath=tmp_path,
        credentials=AWS_CREDENTIALS,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture()
def s3fs_cleanup():
    # clear cache so we get a clean slate every time we instantiate a S3FileSystem
    yield
    S3FileSystem.cachable = False


@pytest.mark.usefixtures("s3fs_cleanup")
class TestNetCDFDataSet:
    def test_temppath_error_raised(self):
        """Test that error is raised if S3 file referenced without a temporary path."""
        pattern = "Need to set temppath in catalog"
        with pytest.raises(ValueError, match=pattern):
            NetCDFDataSet(
                filepath=S3_PATH,
                temppath=None,
            )

    @pytest.mark.parametrize("bad_credentials", [{"key": None, "secret": None}])
    def test_empty_credentials_load(self, bad_credentials, tmp_path):
        netcdf_dataset = NetCDFDataSet(
            filepath=S3_PATH, temppath=tmp_path, credentials=bad_credentials
        )
        pattern = r"Failed while loading data from data set NetCDFDataSet\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            netcdf_dataset.load()

    def test_pass_credentials(self, mocker, tmp_path):
        """Test that AWS credentials are passed successfully into boto3
        client instantiation on creating S3 connection."""
        client_mock = mocker.patch("botocore.session.Session.create_client")
        s3_dataset = NetCDFDataSet(
            filepath=S3_PATH, temppath=tmp_path, credentials=AWS_CREDENTIALS
        )
        pattern = r"Failed while loading data from data set NetCDFDataSet\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            s3_dataset.load()

        assert client_mock.call_count == 1
        args, kwargs = client_mock.call_args_list[0]
        assert args == ("s3",)
        assert kwargs["aws_access_key_id"] == AWS_CREDENTIALS["key"]
        assert kwargs["aws_secret_access_key"] == AWS_CREDENTIALS["secret"]

    @pytest.mark.usefixtures("mocked_s3_bucket")
    def test_save_data(self, s3_dataset, dummy_xr_dataset):
        """Test saving the data to S3."""
        s3_dataset.save(dummy_xr_dataset)
        loaded_data = s3_dataset.load()
        assert_equal(loaded_data, dummy_xr_dataset)

    @pytest.mark.usefixtures("mocked_s3_object")
    def test_load_data(self, s3_dataset, dummy_xr_dataset):
        """Test loading the data from S3."""
        loaded_data = s3_dataset.load()
        assert_equal(loaded_data, dummy_xr_dataset)

    @pytest.mark.usefixtures("mocked_s3_bucket")
    def test_exists(self, s3_dataset, dummy_xr_dataset):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not s3_dataset.exists()
        s3_dataset.save(dummy_xr_dataset)
        assert s3_dataset.exists()

    def test_save_load_locally(self, tmp_path, dummy_xr_dataset):
        """Test loading the data locally."""
        file_path = str(tmp_path / "some" / "dir" / FILE_NAME)
        dataset = NetCDFDataSet(filepath=file_path)

        assert not dataset.exists()
        dataset.save(dummy_xr_dataset)
        assert dataset.exists()
        loaded_data = dataset.load()
        dummy_xr_dataset.equals(loaded_data)

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
