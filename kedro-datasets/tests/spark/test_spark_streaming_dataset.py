import json

import boto3
import pytest
from kedro.io.core import DatasetError
from moto import mock_aws
from packaging.version import Version
from pyspark import __version__
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from kedro_datasets.spark import SparkDataset, SparkStreamingDataset

SCHEMA_FILE_NAME = "schema.json"
BUCKET_NAME = "test_bucket"
AWS_CREDENTIALS = {"key": "FAKE_ACCESS_KEY", "secret": "FAKE_SECRET_KEY"}

SPARK_VERSION = Version(__version__)


def sample_schema(schema_path):
    """read the schema file from json path"""
    with open(schema_path, encoding="utf-8") as f:
        try:
            return StructType.fromJson(json.loads(f.read()))
        except Exception as exc:
            raise DatasetError(
                f"Contents of 'schema.filepath' ({schema_path}) are invalid. "
                f"Schema is required for streaming data load, Please provide a valid schema_path."
            ) from exc


@pytest.fixture
def sample_spark_df_schema() -> StructType:
    """Spark Dataframe schema"""
    return StructType(
        [
            StructField("sku", StringType(), True),
            StructField("new_stock", IntegerType(), True),
        ]
    )


@pytest.fixture
def sample_spark_streaming_df(tmp_path, sample_spark_df_schema):
    """Create a sample dataframe for streaming"""
    data = [("0001", 2), ("0001", 7), ("0002", 4)]
    schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(sample_spark_df_schema.jsonValue(), f)
    return SparkSession.builder.getOrCreate().createDataFrame(
        data, sample_spark_df_schema
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
def s3_bucket():
    with mock_aws():
        s3 = boto3.resource("s3", region_name="us-east-1")
        bucket_name = "test-bucket"
        s3.create_bucket(Bucket=bucket_name)
        yield bucket_name


@pytest.fixture
def mocked_s3_schema(tmp_path, mocked_s3_bucket, sample_spark_df_schema: StructType):
    """Creates schema file and adds it to mocked S3 bucket."""
    temporary_path = tmp_path / SCHEMA_FILE_NAME
    temporary_path.write_text(sample_spark_df_schema.json(), encoding="utf-8")

    mocked_s3_bucket.put_object(
        Bucket=BUCKET_NAME, Key=SCHEMA_FILE_NAME, Body=temporary_path.read_bytes()
    )
    return mocked_s3_bucket


class TestSparkStreamingDataset:
    def test_load(self, tmp_path, sample_spark_streaming_df):
        filepath = (tmp_path / "test_streams").as_posix()
        schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()

        spark_json_ds = SparkDataset(
            filepath=filepath, file_format="json", save_args=[{"mode", "overwrite"}]
        )
        spark_json_ds.save(sample_spark_streaming_df)

        streaming_ds = SparkStreamingDataset(
            filepath=filepath,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
        ).load()
        assert streaming_ds.isStreaming
        schema = sample_schema(schema_path)
        assert streaming_ds.schema == schema

    @pytest.mark.usefixtures("mocked_s3_schema")
    def test_load_options_schema_path_with_credentials(
        self, tmp_path, sample_spark_streaming_df
    ):
        filepath = (tmp_path / "test_streams").as_posix()
        schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()

        spark_json_ds = SparkDataset(
            filepath=filepath, file_format="json", save_args=[{"mode", "overwrite"}]
        )
        spark_json_ds.save(sample_spark_streaming_df)

        streaming_ds = SparkStreamingDataset(
            filepath=filepath,
            file_format="json",
            load_args={
                "schema": {
                    "filepath": f"s3://{BUCKET_NAME}/{SCHEMA_FILE_NAME}",
                    "credentials": AWS_CREDENTIALS,
                }
            },
        ).load()

        assert streaming_ds.isStreaming
        schema = sample_schema(schema_path)
        assert streaming_ds.schema == schema

    def test_save(self, tmp_path, sample_spark_streaming_df):
        filepath_json = (tmp_path / "test_streams").as_posix()
        filepath_output = (tmp_path / "test_streams_output").as_posix()
        schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()
        checkpoint_path = (tmp_path / "checkpoint").as_posix()

        # Save the sample json file to temp_path for creating dataframe
        spark_json_ds = SparkDataset(
            filepath=filepath_json,
            file_format="json",
            save_args=[{"mode", "overwrite"}],
        )
        spark_json_ds.save(sample_spark_streaming_df)

        # Load the json file as the streaming dataframe
        loaded_with_streaming = SparkStreamingDataset(
            filepath=filepath_json,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
        ).load()

        # Append json streams to filepath_output with specified schema path
        streaming_ds = SparkStreamingDataset(
            filepath=filepath_output,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
            save_args={"checkpoint": checkpoint_path, "output_mode": "append"},
        )
        assert not streaming_ds.exists()

        streaming_ds.save(loaded_with_streaming)
        assert streaming_ds.exists()

    def test_exists_raises_error(self, mocker):
        # exists should raise all errors except for
        # AnalysisExceptions clearly indicating a missing file
        spark_dataset = SparkStreamingDataset(filepath="")

        if SPARK_VERSION >= Version("3.4.0"):
            mocker.patch(
                "kedro_datasets.spark.spark_streaming_dataset._get_spark",
                side_effect=AnalysisException("Other Exception"),
            )
        else:
            mocker.patch(
                "kedro_datasets.spark.spark_streaming_dataset._get_spark",
                side_effect=AnalysisException("Other Exception", []),
            )
        with pytest.raises(DatasetError, match="Other Exception"):
            spark_dataset.exists()
