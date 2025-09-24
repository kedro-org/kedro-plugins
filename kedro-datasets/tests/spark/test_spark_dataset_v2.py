"""Tests for SparkDatasetV2."""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest
from kedro.io import DataCatalog, Version
from kedro.io.core import DatasetError, generate_timestamp
from kedro.pipeline import node, pipeline
from kedro.runner import ParallelRunner, SequentialRunner
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from kedro_datasets.pandas import CSVDataset, ParquetDataset
from kedro_datasets.spark import SparkDatasetV2

# Test constants
FILENAME = "test.parquet"
BUCKET_NAME = "test_bucket"
SCHEMA_FILE_NAME = "schema.json"


@pytest.fixture(scope="module")
def spark_session():
    """Create a Spark session for testing."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("TestSparkDatasetV2")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def sample_spark_df(spark_session):
    """Create a sample Spark DataFrame."""
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_pandas_df():
    """Create a sample pandas DataFrame."""
    return pd.DataFrame(
        {
            "name": ["Alice", "Bob", "Charlie"],
            "age": [30, 25, 35],
        }
    )


@pytest.fixture
def sample_schema():
    """Create a sample schema."""
    return StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("height", FloatType(), True),
        ]
    )


@pytest.fixture
def version():
    """Create a version for testing."""
    return Version(None, generate_timestamp())


class TestSparkDatasetV2Basic:
    """Test basic functionality of SparkDatasetV2."""

    def test_load_save_parquet(self, tmp_path, sample_spark_df):
        """Test basic load and save with parquet format."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)

        # Save
        dataset.save(sample_spark_df)
        assert Path(filepath).exists()

        # Load
        loaded_df = dataset.load()
        assert loaded_df.count() == sample_spark_df.count()
        assert set(loaded_df.columns) == set(sample_spark_df.columns)

    def test_load_save_csv(self, tmp_path, sample_spark_df):
        """Test load and save with CSV format."""
        filepath = str(tmp_path / "test.csv")
        dataset = SparkDatasetV2(
            filepath=filepath,
            file_format="csv",
            save_args={"header": True},
            load_args={"header": True, "inferSchema": True},
        )

        dataset.save(sample_spark_df)
        loaded_df = dataset.load()

        assert loaded_df.count() == sample_spark_df.count()
        assert set(loaded_df.columns) == set(sample_spark_df.columns)

    def test_load_save_json(self, tmp_path, sample_spark_df):
        """Test load and save with JSON format."""
        filepath = str(tmp_path / "test.json")
        dataset = SparkDatasetV2(filepath=filepath, file_format="json")

        dataset.save(sample_spark_df)
        loaded_df = dataset.load()

        assert loaded_df.count() == sample_spark_df.count()

    def test_save_modes(self, tmp_path, sample_spark_df):
        """Test different save modes."""
        filepath = str(tmp_path / "test.parquet")

        # Test overwrite mode
        dataset = SparkDatasetV2(filepath=filepath, save_args={"mode": "overwrite"})
        dataset.save(sample_spark_df)
        dataset.save(sample_spark_df)  # Should not fail

        # Test append mode
        dataset_append = SparkDatasetV2(
            filepath=str(tmp_path / "test_append.parquet"), save_args={"mode": "append"}
        )
        dataset_append.save(sample_spark_df)
        dataset_append.save(sample_spark_df)
        loaded = dataset_append.load()
        assert loaded.count() == sample_spark_df.count() * 2

    def test_partitioning(self, tmp_path, sample_spark_df):
        """Test data partitioning."""
        filepath = str(tmp_path / "test_partitioned.parquet")
        dataset = SparkDatasetV2(filepath=filepath, save_args={"partitionBy": ["name"]})

        dataset.save(sample_spark_df)

        # Check partition directories exist
        base_path = Path(filepath)
        partitions = [d for d in base_path.iterdir() if d.is_dir()]
        assert len(partitions) > 0
        assert any("name=" in d.name for d in partitions)

    def test_exists(self, tmp_path, sample_spark_df):
        """Test exists functionality."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)

        assert not dataset.exists()
        dataset.save(sample_spark_df)
        assert dataset.exists()

    def test_describe(self, tmp_path):
        """Test _describe method."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(
            filepath=filepath,
            file_format="parquet",
            load_args={"mergeSchema": True},
            save_args={"compression": "snappy"},
        )

        description = dataset._describe()
        assert description["file_format"] == "parquet"
        assert description["load_args"] == {"mergeSchema": True}
        assert description["save_args"] == {"compression": "snappy"}

    def test_str_representation(self, tmp_path):
        """Test string representation."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)

        assert "SparkDatasetV2" in str(dataset)
        assert filepath in str(dataset)


class TestSparkDatasetV2Schema:
    """Test schema handling in SparkDatasetV2."""

    def test_schema_from_dict(self, tmp_path, sample_pandas_df, sample_schema):
        """Test loading schema from dict."""
        # Save schema to file
        schema_path = tmp_path / "schema.json"
        schema_path.write_text(sample_schema.json())

        # Save CSV data
        csv_path = str(tmp_path / "test.csv")
        sample_pandas_df.to_csv(csv_path, index=False)

        # Load with schema
        dataset = SparkDatasetV2(
            filepath=csv_path,
            file_format="csv",
            load_args={"header": True, "schema": {"filepath": str(schema_path)}},
        )

        loaded_df = dataset.load()
        assert loaded_df.schema == sample_schema

    def test_schema_invalid_filepath(self, tmp_path):
        """Test error when schema filepath is invalid."""
        csv_path = str(tmp_path / "test.csv")
        schema_path = tmp_path / "bad_schema.json"
        schema_path.write_text("invalid json {")

        with pytest.raises(DatasetError, match="Failed to load schema"):
            SparkDatasetV2(
                filepath=csv_path,
                file_format="csv",
                load_args={"schema": {"filepath": str(schema_path)}},
            )

    def test_schema_missing_filepath(self, tmp_path):
        """Test error when schema dict missing filepath."""
        csv_path = str(tmp_path / "test.csv")

        with pytest.raises(DatasetError, match="Schema dict must have 'filepath'"):
            SparkDatasetV2(
                filepath=csv_path, file_format="csv", load_args={"schema": {}}
            )


class TestSparkDatasetV2PathHandling:
    """Test path handling in SparkDatasetV2."""

    def test_local_path(self, tmp_path):
        """Test local path handling."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)

        assert dataset.protocol == ""
        assert dataset._spark_path == filepath

    def test_s3_path_normalization(self):
        """Test S3 path normalization to s3a://."""
        # All S3 variants should normalize to s3a://
        for prefix in ["s3://", "s3n://", "s3a://"]:
            filepath = f"{prefix}bucket/path/data.parquet"
            dataset = SparkDatasetV2(filepath=filepath)
            assert dataset._spark_path.startswith("s3a://")

    @pytest.mark.skipif(
        "DATABRICKS_RUNTIME_VERSION" not in os.environ,
        reason="Not running on Databricks",
    )
    def test_dbfs_path_on_databricks(self):
        """Test DBFS path handling on Databricks."""
        filepath = "/dbfs/path/to/data.parquet"
        dataset = SparkDatasetV2(filepath=filepath)
        assert dataset._spark_path == "dbfs:/path/to/data.parquet"

    def test_dbfs_path_not_on_databricks(self, monkeypatch):
        """Test DBFS path handling when not on Databricks."""
        # Ensure we're not on Databricks
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

        filepath = "/dbfs/path/to/data.parquet"
        dataset = SparkDatasetV2(filepath=filepath)
        assert dataset._spark_path == filepath

    def test_other_protocols(self):
        """Test other protocol handling."""
        protocols = {
            "gs://bucket/path": "gs://",
            "abfs://container@account.dfs.core.windows.net/path": "abfs://",
        }

        for filepath, expected_prefix in protocols.items():
            dataset = SparkDatasetV2(filepath=filepath)
            assert dataset._spark_path.startswith(expected_prefix)


class TestSparkDatasetV2ErrorMessages:
    """Test improved error messages in SparkDatasetV2."""

    def test_missing_s3fs_error(self, mocker):
        """Test helpful error for missing s3fs."""
        import_error = ImportError("No module named 's3fs'")
        mocker.patch("fsspec.filesystem", side_effect=import_error)

        with pytest.raises(
            ImportError, match="pip install 'kedro-datasets\\[spark-s3\\]'"
        ):
            SparkDatasetV2(filepath="s3://bucket/data.parquet")

    def test_missing_gcsfs_error(self, mocker):
        """Test helpful error for missing gcsfs."""
        import_error = ImportError("No module named 'gcsfs'")
        mocker.patch("fsspec.filesystem", side_effect=import_error)

        with pytest.raises(ImportError, match="pip install gcsfs"):
            SparkDatasetV2(filepath="gs://bucket/data.parquet")

    def test_missing_pyspark_databricks(self, mocker, monkeypatch):
        """Test helpful error for PySpark on Databricks."""
        monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")

        dataset = SparkDatasetV2(filepath="test.parquet")
        mocker.patch.object(
            dataset, "_get_spark", side_effect=ImportError("No module named 'pyspark'")
        )

        with pytest.raises(ImportError, match="databricks-connect"):
            dataset.load()

    def test_missing_pyspark_emr(self, mocker, monkeypatch):
        """Test helpful error for PySpark on EMR."""
        monkeypatch.setenv("EMR_RELEASE_LABEL", "emr-7.0.0")

        dataset = SparkDatasetV2(filepath="test.parquet")
        mocker.patch.object(
            dataset, "_get_spark", side_effect=ImportError("No module named 'pyspark'")
        )

        with pytest.raises(ImportError, match="should be pre-installed on EMR"):
            dataset.load()

    def test_missing_pyspark_local(self, mocker, monkeypatch):
        """Test helpful error for PySpark locally."""
        monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
        monkeypatch.delenv("EMR_RELEASE_LABEL", raising=False)

        dataset = SparkDatasetV2(filepath="test.parquet")
        mocker.patch.object(
            dataset, "_get_spark", side_effect=ImportError("No module named 'pyspark'")
        )

        with pytest.raises(
            ImportError, match="pip install 'kedro-datasets\\[spark-local\\]'"
        ):
            dataset.load()


class TestSparkDatasetV2Delta:
    """Test Delta format handling in SparkDatasetV2."""

    @pytest.mark.parametrize("mode", ["merge", "update", "delete"])
    def test_delta_unsupported_modes(self, tmp_path, mode):
        """Test that unsupported Delta modes raise errors."""
        filepath = str(tmp_path / "test.delta")

        with pytest.raises(
            DatasetError, match=f"Delta format doesn't support mode '{mode}'"
        ):
            SparkDatasetV2(
                filepath=filepath, file_format="delta", save_args={"mode": mode}
            )

    @pytest.mark.parametrize(
        "mode", ["append", "overwrite", "error", "errorifexists", "ignore"]
    )
    def test_delta_supported_modes(self, tmp_path, mode):
        """Test that supported Delta modes work."""
        filepath = str(tmp_path / "test.delta")

        # Should not raise
        dataset = SparkDatasetV2(
            filepath=filepath, file_format="delta", save_args={"mode": mode}
        )
        assert dataset.file_format == "delta"


class TestSparkDatasetV2Versioning:
    """Test versioning functionality in SparkDatasetV2."""

    def test_versioned_save_and_load(self, tmp_path, sample_spark_df, version):
        """Test versioned save and load."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath, version=version)

        # Save versioned
        dataset.save(sample_spark_df)

        # Check versioned path exists
        versioned_path = tmp_path / "test.parquet" / version.save / "test.parquet"
        assert versioned_path.exists()

        # Load versioned
        loaded_df = dataset.load()
        assert loaded_df.count() == sample_spark_df.count()

    def test_no_version_error(self, tmp_path):
        """Test error when no versions exist."""
        filepath = str(tmp_path / "test.parquet")
        version = Version(None, None)  # Load latest
        dataset = SparkDatasetV2(filepath=filepath, version=version)

        with pytest.raises(DatasetError, match="Did not find any versions"):
            dataset.load()

    def test_version_str_representation(self, tmp_path, version):
        """Test version in string representation."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath, version=version)

        assert "version=" in str(dataset._describe())


class TestSparkDatasetV2Integration:
    """Integration tests for SparkDatasetV2."""

    def test_parallel_runner_restriction(self, tmp_path, sample_spark_df):
        """Test that ParallelRunner is restricted."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)
        dataset.save(sample_spark_df)

        catalog = DataCatalog({"spark_data": dataset})
        test_pipeline = pipeline([node(lambda x: x, "spark_data", "output")])

        with pytest.raises(AttributeError, match="cannot be used with multiprocessing"):
            ParallelRunner().run(test_pipeline, catalog)

    def test_sequential_runner(self, tmp_path, sample_spark_df):
        """Test that SequentialRunner works."""
        filepath_in = str(tmp_path / "input.parquet")
        filepath_out = str(tmp_path / "output.parquet")

        dataset_in = SparkDatasetV2(filepath=filepath_in)
        dataset_out = SparkDatasetV2(filepath=filepath_out)
        dataset_in.save(sample_spark_df)

        catalog = DataCatalog({"input": dataset_in, "output": dataset_out})

        test_pipeline = pipeline([node(lambda x: x, "input", "output")])
        SequentialRunner().run(test_pipeline, catalog)

        assert Path(filepath_out).exists()

    def test_interop_with_pandas_dataset(self, tmp_path, sample_pandas_df):
        """Test interoperability with pandas datasets."""
        # Save with pandas
        pandas_path = str(tmp_path / "pandas.parquet")
        pandas_dataset = ParquetDataset(filepath=pandas_path)
        pandas_dataset.save(sample_pandas_df)

        # Load with SparkDatasetV2
        spark_dataset = SparkDatasetV2(filepath=pandas_path)
        spark_df = spark_dataset.load()

        assert spark_df.count() == len(sample_pandas_df)
        assert set(spark_df.columns) == set(sample_pandas_df.columns)


class TestSparkDatasetV2Compatibility:
    """Test compatibility between V1 and V2."""

    def test_v1_v2_coexistence(self, tmp_path, sample_spark_df):
        """Test that V1 and V2 can coexist in the same catalog."""
        from kedro_datasets.spark import SparkDataset  # noqa: PLC0415

        filepath_v1 = str(tmp_path / "v1.parquet")
        filepath_v2 = str(tmp_path / "v2.parquet")

        dataset_v1 = SparkDataset(filepath=filepath_v1)
        dataset_v2 = SparkDatasetV2(filepath=filepath_v2)

        catalog = DataCatalog(
            {
                "v1_data": dataset_v1,
                "v2_data": dataset_v2,
            }
        )

        # Both should work
        dataset_v1.save(sample_spark_df)
        dataset_v2.save(sample_spark_df)

        assert catalog.exists("v1_data")
        assert catalog.exists("v2_data")

    def test_v2_reads_v1_data(self, tmp_path, sample_spark_df):
        """Test that V2 can read data saved by V1."""
        from kedro_datasets.spark import SparkDataset  # noqa: PLC0415

        filepath = str(tmp_path / "shared.parquet")

        # V1 saves
        dataset_v1 = SparkDataset(filepath=filepath)
        dataset_v1.save(sample_spark_df)

        # V2 loads
        dataset_v2 = SparkDatasetV2(filepath=filepath)
        loaded_df = dataset_v2.load()

        assert loaded_df.count() == sample_spark_df.count()


# Fixtures for mocking cloud storage
@pytest.fixture
def mock_s3_filesystem():
    """Mock S3 filesystem."""
    with patch("fsspec.filesystem") as mock_fs:
        mock_filesystem = MagicMock()
        mock_filesystem.exists.return_value = True
        mock_filesystem.glob.return_value = []
        mock_fs.return_value = mock_filesystem
        yield mock_filesystem


class TestSparkDatasetV2CloudStorage:
    """Test cloud storage handling."""

    def test_s3_credentials(self, mock_s3_filesystem):
        """Test S3 with credentials."""
        dataset = SparkDatasetV2(
            filepath="s3://bucket/data.parquet",
            credentials={"key": "test_key", "secret": "test_secret"},
        )

        # Verify s3a:// normalization
        assert dataset._spark_path.startswith("s3a://")

    def test_gcs_handling(self, mock_s3_filesystem):
        """Test GCS handling."""
        dataset = SparkDatasetV2(
            filepath="gs://bucket/data.parquet",
            credentials={"token": "path/to/token.json"},
        )

        assert dataset._spark_path.startswith("gs://")

    def test_azure_handling(self, mock_s3_filesystem):
        """Test Azure Blob Storage handling."""
        dataset = SparkDatasetV2(
            filepath="abfs://container@account.dfs.core.windows.net/data.parquet",
            credentials={"account_key": "test_key"},
        )

        assert dataset._spark_path.startswith("abfs://")
