import boto3
import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_s3
from pandas.testing import assert_frame_equal
from s3fs import S3FileSystem

from kedro_datasets._io import DatasetError
from kedro_datasets.dask import ParquetDataset

FILE_NAME = "test.parquet"
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
def dummy_dd_dataframe() -> dd.DataFrame:
    df = pd.DataFrame(
        {"Name": ["Alex", "Bob", "Clarke", "Dave"], "Age": [31, 12, 65, 29]}
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.fixture
def mocked_s3_object(tmp_path, mocked_s3_bucket, dummy_dd_dataframe: dd.DataFrame):
    """Creates test data and adds it to mocked S3 bucket."""
    pandas_df = dummy_dd_dataframe.compute()
    table = pa.Table.from_pandas(pandas_df)
    temporary_path = tmp_path / FILE_NAME
    pq.write_table(table, str(temporary_path))

    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME, Key=FILE_NAME, Body=temporary_path.read_bytes()
    )
    return mocked_s3_bucket


@pytest.fixture
def s3_dataset(load_args, save_args):
    return ParquetDataset(
        filepath=S3_PATH,
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
class TestParquetDataset:
    def test_incorrect_credentials_load(self):
        """Test that incorrect credential keys won't instantiate dataset."""
        pattern = r"unexpected keyword argument"
        with pytest.raises(DatasetError, match=pattern):
            ParquetDataset(
                filepath=S3_PATH,
                credentials={
                    "client_kwargs": {"access_token": "TOKEN", "access_key": "KEY"}
                },
            ).load().compute()

    @pytest.mark.parametrize("bad_credentials", [{"key": None, "secret": None}])
    def test_empty_credentials_load(self, bad_credentials):
        parquet_dataset = ParquetDataset(filepath=S3_PATH, credentials=bad_credentials)
        pattern = r"Failed while loading data from data set ParquetDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            parquet_dataset.load().compute()

    @pytest.mark.xfail
    def test_pass_credentials(self, mocker):
        """Test that AWS credentials are passed successfully into boto3
        client instantiation on creating S3 connection."""
        client_mock = mocker.patch("botocore.session.Session.create_client")
        s3_dataset = ParquetDataset(filepath=S3_PATH, credentials=AWS_CREDENTIALS)
        pattern = r"Failed while loading data from data set ParquetDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            s3_dataset.load().compute()

        assert client_mock.call_count == 1
        args, kwargs = client_mock.call_args_list[0]
        assert args == ("s3",)
        assert kwargs["aws_access_key_id"] == AWS_CREDENTIALS["key"]
        assert kwargs["aws_secret_access_key"] == AWS_CREDENTIALS["secret"]

    def test_save_data(self, s3_dataset, mocked_s3_bucket):
        """Test saving the data to S3."""
        pd_data = pd.DataFrame(
            {"col1": ["a", "b"], "col2": ["c", "d"], "col3": ["e", "f"]}
        )
        dd_data = dd.from_pandas(pd_data, npartitions=1)
        s3_dataset.save(dd_data)
        loaded_data = s3_dataset.load()
        assert_frame_equal(loaded_data.compute(), dd_data.compute())

    def test_load_data(self, s3_dataset, dummy_dd_dataframe, mocked_s3_object):
        """Test loading the data from S3."""
        loaded_data = s3_dataset.load()
        assert_frame_equal(loaded_data.compute(), dummy_dd_dataframe.compute())

    def test_exists(self, s3_dataset, dummy_dd_dataframe, mocked_s3_bucket):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not s3_dataset.exists()
        s3_dataset.save(dummy_dd_dataframe)
        assert s3_dataset.exists()

    def test_save_load_locally(self, tmp_path, dummy_dd_dataframe):
        """Test loading the data locally."""
        file_path = str(tmp_path / "some" / "dir" / FILE_NAME)
        dataset = ParquetDataset(filepath=file_path)

        assert not dataset.exists()
        dataset.save(dummy_dd_dataframe)
        assert dataset.exists()
        loaded_data = dataset.load()
        dummy_dd_dataframe.compute().equals(loaded_data.compute())

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
        s3_dataset._process_schema()
        assert s3_dataset._save_args.get("schema") is None

        for key, value in save_args.items():
            assert s3_dataset._save_args[key] == value

        for key, value in s3_dataset.DEFAULT_SAVE_ARGS.items():
            assert s3_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "save_args",
        [{"schema": {"col1": "[[int64]]", "col2": "string"}}],
        indirect=True,
    )
    def test_save_extra_params_schema_dict(self, s3_dataset, save_args):
        """Test setting the schema as dictionary of pyarrow column types
        in save arguments."""

        for key, value in save_args["schema"].items():
            assert s3_dataset._save_args["schema"][key] == value

        s3_dataset._process_schema()

        for field in s3_dataset._save_args["schema"].values():
            assert isinstance(field, pa.DataType)

    @pytest.mark.parametrize(
        "save_args",
        [
            {
                "schema": {
                    "col1": "[[int64]]",
                    "col2": "string",
                    "col3": float,
                    "col4": pa.int64(),
                }
            }
        ],
        indirect=True,
    )
    def test_save_extra_params_schema_dict_mixed_types(self, s3_dataset, save_args):
        """Test setting the schema as dictionary of mixed value types
        in save arguments."""

        for key, value in save_args["schema"].items():
            assert s3_dataset._save_args["schema"][key] == value

        s3_dataset._process_schema()

        for field in s3_dataset._save_args["schema"].values():
            assert isinstance(field, pa.DataType)

    @pytest.mark.parametrize(
        "save_args",
        [{"schema": "c1:[int64],c2:int64"}],
        indirect=True,
    )
    def test_save_extra_params_schema_str_schema_fields(self, s3_dataset, save_args):
        """Test setting the schema as string pyarrow schema (list of fields)
        in save arguments."""

        assert s3_dataset._save_args["schema"] == save_args["schema"]

        s3_dataset._process_schema()

        assert isinstance(s3_dataset._save_args["schema"], pa.Schema)
