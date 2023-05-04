import json

import pytest
from kedro.io.core import DataSetError
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kedro_datasets.spark.spark_dataset import SparkDataSet
from kedro_datasets.spark.spark_streaming_dataset import SparkStreamingDataSet

SCHEMA_FILE_NAME = "schema.json"


def sample_schema(schema_path):
    with open(schema_path, encoding="utf-8") as f:
        try:
            return StructType.fromJson(json.loads(f.read()))
        except Exception as exc:
            raise DataSetError(
                f"Contents of 'schema.filepath' ({schema_path}) are invalid. "
                f"Schema is required for streaming data load, Please provide a valid schema_path."
            ) from exc


@pytest.fixture
def sample_spark_streaming_df(tmp_path):
    schema = StructType(
        [
            StructField("sku", StringType(), True),
            StructField("new_stock", IntegerType(), True),
        ]
    )
    data = [("0001", 2), ("0001", 7), ("0002", 4)]
    schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema.jsonValue(), f)
    return SparkSession.builder.getOrCreate().createDataFrame(data, schema)


class TestStreamingDataSet:
    def test_load(self, tmp_path, sample_spark_streaming_df):
        filepath = (tmp_path / "test_streams").as_posix()
        schema_path = (tmp_path / SCHEMA_FILE_NAME).as_posix()

        spark_json_ds = SparkDataSet(
            filepath=filepath, file_format="json", save_args=[{"mode", "overwrite"}]
        )
        spark_json_ds.save(sample_spark_streaming_df)

        streaming_ds = SparkStreamingDataSet(
            filepath=filepath,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
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
        spark_json_ds = SparkDataSet(
            filepath=filepath_json,
            file_format="json",
            save_args=[{"mode", "overwrite"}],
        )
        spark_json_ds.save(sample_spark_streaming_df)

        # Load the json file as the streaming dataframe
        loaded_with_streaming = SparkStreamingDataSet(
            filepath=filepath_json,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
        ).load()

        # Append json streams to filepath_output with specified schema path
        streaming_ds = SparkStreamingDataSet(
            filepath=filepath_output,
            file_format="json",
            load_args={"schema": {"filepath": schema_path}},
            save_args={"checkpoint": checkpoint_path, "output_mode": "append"},
        )
        assert not streaming_ds.exists()

        streaming_ds.save(loaded_with_streaming)
        assert streaming_ds.exists()
<<<<<<< HEAD

    def test_exists_raises_error(self, mocker):
        # exists should raise all errors except for
        # AnalysisExceptions clearly indicating a missing file
        spark_data_set = SparkStreamingDataSet(filepath="")
        mocker.patch.object(
            spark_data_set,
            "_get_spark",
            side_effect=AnalysisException("Other Exception", []),
        )

        with pytest.raises(DataSetError, match="Other Exception"):
            spark_data_set.exists()
=======
>>>>>>> d1472e2 (update test and remove redundacy)
