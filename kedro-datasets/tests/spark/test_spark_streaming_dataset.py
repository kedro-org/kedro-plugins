import pytest
import time
from pyspark.sql import SparkSession
from kedro_datasets.spark import SparkStreamingDataSet,SparkDataSet
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


@pytest.fixture
def sample_spark_streaming_df_one():
    schema = StructType(
        [
            StructField("sku", StringType(), True),
            StructField("new_stock", IntegerType(), True),
        ]
    )
    data = [("0001", 2), ("0001", 7), ("0002", 4)]

    return SparkSession.builder.getOrCreate() \
            .createDataFrame(data, schema)


class TestStreamingDataSet:
    def test_load(self,tmp_path, sample_spark_streaming_df_one):
        filepath = (tmp_path / "test_streams").as_posix()
        spark_json_ds = SparkDataSet(filepath=filepath, file_format="json",save_args=["mode","overwrite"])
        spark_json_ds.save(sample_spark_streaming_df_one)
        loaded_with_spark = spark_json_ds.load()

        stream_df = SparkStreamingDataSet(filepath=filepath, file_format="json")._load()
        assert stream_df.isStreaming

        stream_query = stream_df.writeStream.format("memory").queryName("test").start()
        assert stream_query.isActive
        time.sleep(3)
        stream_query.stop()
        loaded_memory_stream = SparkSession.builder.getOrCreate().sql("select * from test")

        assert loaded_memory_stream.exceptAll(loaded_with_spark).count()==0


    def test_save(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_streams").as_posix()
        checkpoint_path = (tmp_path / "checkpoint").as_posix()
        streaming_ds = SparkStreamingDataSet(filepath=filepath, save_args=["checkpointLocation",checkpoint_path])
        assert not streaming_ds.exists()


