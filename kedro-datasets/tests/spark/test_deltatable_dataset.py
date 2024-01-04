import pytest
from delta import DeltaTable
from kedro.io import DataCatalog
from kedro.io.core import DatasetError
from kedro.pipeline import node
from kedro.pipeline.modular_pipeline import pipeline as modular_pipeline
from kedro.runner import ParallelRunner
from packaging.version import Version
from pyspark import __version__
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.sql.utils import AnalysisException

from kedro_datasets.spark import DeltaTableDataset, SparkDataset

SPARK_VERSION = Version(__version__)


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


class TestDeltaTableDataset:
    def test_load(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        spark_delta_ds = SparkDataset(filepath=filepath, file_format="delta")
        spark_delta_ds.save(sample_spark_df)
        loaded_with_spark = spark_delta_ds.load()
        assert loaded_with_spark.exceptAll(sample_spark_df).count() == 0

        delta_ds = DeltaTableDataset(filepath=filepath)
        delta_table = delta_ds.load()

        assert isinstance(delta_table, DeltaTable)
        loaded_with_deltalake = delta_table.toDF()
        assert loaded_with_deltalake.exceptAll(loaded_with_spark).count() == 0

    def test_save(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        delta_ds = DeltaTableDataset(filepath=filepath)
        assert not delta_ds.exists()

        pattern = "DeltaTableDataset is a read only dataset type"
        with pytest.raises(DatasetError, match=pattern):
            delta_ds.save(sample_spark_df)

        # check that indeed nothing is written
        assert not delta_ds.exists()

    def test_exists(self, tmp_path, sample_spark_df):
        filepath = (tmp_path / "test_data").as_posix()
        delta_ds = DeltaTableDataset(filepath=filepath)

        assert not delta_ds.exists()

        spark_delta_ds = SparkDataset(filepath=filepath, file_format="delta")
        spark_delta_ds.save(sample_spark_df)

        assert delta_ds.exists()

    def test_exists_raises_error(self, mocker):
        delta_ds = DeltaTableDataset(filepath="")
        if SPARK_VERSION >= Version("3.4.0"):
            mocker.patch(
                "kedro_datasets.spark.deltatable_dataset._get_spark",
                side_effect=AnalysisException("Other Exception"),
            )
        else:
            mocker.patch(
                "kedro_datasets.spark.deltatable_dataset._get_spark",
                side_effect=AnalysisException("Other Exception", []),
            )
        with pytest.raises(DatasetError, match="Other Exception"):
            delta_ds.exists()

    @pytest.mark.parametrize("is_async", [False, True])
    def test_parallel_runner(self, is_async):
        """Test ParallelRunner with SparkDataset fails."""

        def no_output(x):
            _ = x + 1  # pragma: no cover

        delta_ds = DeltaTableDataset(filepath="")
        catalog = DataCatalog({"delta_in": delta_ds})
        pipeline = modular_pipeline([node(no_output, "delta_in", None)])
        pattern = (
            r"The following data sets cannot be used with "
            r"multiprocessing: \['delta_in'\]"
        )
        with pytest.raises(AttributeError, match=pattern):
            ParallelRunner(is_async=is_async).run(pipeline, catalog)
