import pytest
from kedro.io.core import DatasetError
from pyspark.sql import DataFrame

from kedro_datasets_experimental.databricks.external_table_dataset import (
    ExternalTableDataset,
)


class TestExternalTableDataset:
    def test_location_for_non_existing_table(self):
        with pytest.raises(DatasetError):
            ExternalTableDataset(table="test")

    def test_invalid_upsert_write_mode(self):
        with pytest.raises(DatasetError):
            ExternalTableDataset(table="test", write_mode="upsert", format="parquet")

    def test_invalid_overwrite_write_mode(self):
        with pytest.raises(DatasetError):
            ExternalTableDataset(table="test", write_mode="overwrite", format="parquet")

    def test_save_overwrite_without_location(self):
        with pytest.raises(DatasetError):
            ExternalTableDataset(table="test", write_mode="overwrite", format="delta")

    def test_save_overwrite(
        self,
        sample_spark_df: DataFrame,
        append_spark_df: DataFrame,
        external_location: str,
    ):
        unity_ds = ExternalTableDataset(
            database="test",
            table="test_save",
            format="parquet",
            write_mode="overwrite",
            location=f"{external_location}/test_save_overwrite_external",
        )
        unity_ds.save(sample_spark_df)
        unity_ds.save(append_spark_df)

        overwritten_table = unity_ds.load()

        assert append_spark_df.exceptAll(overwritten_table).count() == 0
