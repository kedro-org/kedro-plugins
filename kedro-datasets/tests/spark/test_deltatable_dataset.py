import pytest
from deltalake import DeltaTable
from kedro.io import DataCatalog, DataSetError
from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline as modular_pipeline
from kedro.runner import ParallelRunner
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kedro_datasets.spark import DeltaTableDataSet, SparkDataSet


@pytest.fixture
def sample_spark_df():
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    data = [("Alex", 31), ("Bob", 12), ("Clarke", 65), ("Dave", 29)]

    return SparkSession.builder.getOrCreate().createDataFrame(data, schema)


class TestDeltaTableDataSet:
    def test_load(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        spark_delta_ds = SparkDataSet(filepath=filepath, file_format="delta")
        spark_delta_ds.save(sample_spark_df)
        loaded_with_spark = spark_delta_ds.load()
        assert loaded_with_spark.exceptAll(sample_spark_df).count() == 0

        delta_ds = DeltaTableDataSet(filepath=filepath)
        delta_table = delta_ds.load()

        assert isinstance(delta_table, DeltaTable)
        loaded_with_deltalake = delta_table.to_pandas()
        loaded_with_spark = loaded_with_spark.toPandas()
        assert len(loaded_with_spark) == len(loaded_with_deltalake)

    def test_save(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        delta_ds = DeltaTableDataSet(filepath=filepath)
        assert not delta_ds.exists()

        pattern = "DeltaTableDataSet is a read only dataset type"
        with pytest.raises(DataSetError, match=pattern):
            delta_ds.save(sample_spark_df)

        # check that indeed nothing is written
        assert not delta_ds.exists()

    def test_exists(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        delta_ds = DeltaTableDataSet(filepath=filepath)

        assert not delta_ds.exists()

        spark_delta_ds = SparkDataSet(filepath=filepath, file_format="delta")
        spark_delta_ds.save(sample_spark_df)

        assert delta_ds.exists()

    @pytest.mark.parametrize("is_async", [False, True])
    def test_parallel_runner(self, is_async):
        """Test ParallelRunner with SparkDataSet fails."""

        def no_output(x):
            _ = x + 1  # pragma: no cover

        delta_ds = DeltaTableDataSet(filepath="")
        catalog = DataCatalog(data_sets={"delta_in": delta_ds})
        pipeline = modular_pipeline([node(no_output, "delta_in", None)])
        pattern = (
            r"The following data sets cannot be used with "
            r"multiprocessing: \['delta_in'\]"
        )
        with pytest.raises(AttributeError, match=pattern):
            ParallelRunner(is_async=is_async).run(pipeline, catalog)
