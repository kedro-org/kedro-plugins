import importlib
from io import BytesIO

import dask.dataframe as dd
import pandas as pd
import pyarrow as pa
import pytest
from pandas.testing import assert_frame_equal

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets._io import DatasetError
from kedro_datasets.dask import ParquetDataset
from kedro_datasets.dask.parquet_dataset import _DEPRECATED_CLASSES

FILE_NAME = "test.parquet"
BUCKET_NAME = "test_bucket"

# Pathlib cannot be used since it strips out the second slash from "s3://"
S3_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"


@pytest.fixture
def dummy_dd_dataframe() -> dd.DataFrame:
    df = pd.DataFrame(
        {"Name": ["Alex", "Bob", "Clarke", "Dave"], "Age": [31, 12, 65, 29]}
    )
    return dd.from_pandas(df, npartitions=1)


@pytest.fixture
def s3_dataset(
    mocked_s3_bucket, credentials, mock_fs_args, save_args, load_args
):  # pylint: disable=unused-argument
    return ParquetDataset(
        filepath=S3_PATH,
        credentials=credentials,
        fs_args=mock_fs_args,
        load_args=load_args,
        save_args=save_args,
    )


@pytest.fixture
def mocked_parquet_in_s3(mocked_s3_bucket, dummy_dd_dataframe):
    pandas_df = dummy_dd_dataframe.compute()
    buffer = BytesIO()
    pandas_df.to_parquet(buffer)
    buffer.seek(0)
    mocked_s3_bucket.put_object(Bucket=BUCKET_NAME, Key=FILE_NAME, Body=buffer)
    return S3_PATH


@pytest.mark.parametrize(
    "module_name", ["kedro_datasets.dask", "kedro_datasets.dask.parquet_dataset"]
)
@pytest.mark.parametrize("class_name", _DEPRECATED_CLASSES)
def test_deprecation(module_name, class_name):
    with pytest.warns(
        KedroDeprecationWarning, match=f"{repr(class_name)} has been renamed"
    ):
        getattr(importlib.import_module(module_name), class_name)


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

    def test_exists(self, s3_dataset, dummy_dd_dataframe):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not s3_dataset.exists()
        s3_dataset.save(dummy_dd_dataframe)
        assert s3_dataset.exists()

    def test_load_data(
        self, mocked_parquet_in_s3, mock_fs_args, credentials, dummy_dd_dataframe
    ):
        """Test loading the data from S3."""
        dataset = ParquetDataset(
            filepath=mocked_parquet_in_s3,
            credentials=credentials,
            fs_args=mock_fs_args,
        )
        loaded_data = dataset.load()
        assert_frame_equal(loaded_data.compute(), dummy_dd_dataframe.compute())

    def test_save_data(self, s3_dataset):
        """Test saving the data to S3."""
        pd_data = pd.DataFrame(
            {"col1": ["a", "b"], "col2": ["c", "d"], "col3": ["e", "f"]}
        )
        dd_data = dd.from_pandas(pd_data, npartitions=1)
        s3_dataset.save(dd_data)
        loaded_data = s3_dataset.load()
        assert_frame_equal(loaded_data.compute(), dd_data.compute())

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
