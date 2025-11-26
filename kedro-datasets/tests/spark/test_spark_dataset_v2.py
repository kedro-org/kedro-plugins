"""Tests for SparkDatasetV2."""

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from kedro.io import DataCatalog, Version
from kedro.io.core import DatasetError, generate_timestamp
from kedro.pipeline import node, pipeline
from kedro.runner import SequentialRunner
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from kedro_datasets._utils.databricks_utils import (
    parse_spark_filepath,
    to_spark_path,
)
from kedro_datasets.pandas import ParquetDataset
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

        str_repr = str(dataset)
        assert "SparkDatasetV2" in str_repr
        # The filepath in str representation is the spark path (file://)
        assert "file://" in str_repr or filepath in str_repr

    def test_relative_path(self, sample_spark_df):
        """Test that relative paths work correctly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Change to temp directory
            original_cwd = os.getcwd()
            try:
                os.chdir(tmp_dir)

                # Use relative path
                relative_path = "data/test.parquet"
                dataset = SparkDatasetV2(filepath=relative_path)

                # Save and load
                dataset.save(sample_spark_df)

                # Check file exists at resolved path
                expected_path = Path(tmp_dir) / "data" / "test.parquet"
                assert expected_path.exists()

                # Load should work
                loaded_df = dataset.load()
                assert loaded_df.count() == sample_spark_df.count()
            finally:
                os.chdir(original_cwd)


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

        with pytest.raises(
            DatasetError, match="Contents of 'schema.filepath'.*are invalid"
        ):
            SparkDatasetV2(
                filepath=csv_path,
                file_format="csv",
                load_args={"schema": {"filepath": str(schema_path)}},
            )

    def test_schema_missing_filepath(self, tmp_path):
        """Test error when schema dict missing filepath."""
        csv_path = str(tmp_path / "test.csv")

        with pytest.raises(
            DatasetError, match="Schema load argument does not specify a 'filepath'"
        ):
            SparkDatasetV2(
                filepath=csv_path, file_format="csv", load_args={"schema": {}}
            )

    def test_schema_as_structtype(self, tmp_path, sample_spark_df, sample_schema):
        """Test passing schema directly as StructType."""
        filepath = str(tmp_path / "test.parquet")

        # Create dataset with StructType schema directly
        dataset = SparkDatasetV2(
            filepath=filepath,
            file_format="parquet",
            load_args={"schema": sample_schema},
        )

        # The schema should be stored correctly
        assert dataset._schema == sample_schema


class TestSparkDatasetV2PathHandling:
    """Test path handling in SparkDatasetV2."""

    def test_local_path(self, tmp_path):
        """Test local path handling."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)

        # Test the utility function
        protocol, path = parse_spark_filepath(filepath)
        assert protocol == "file"
        assert dataset._spark_path == f"file://{filepath}"

    def test_s3_path_normalization(self):
        """Test S3 path normalization to s3a://."""
        # All S3 variants should normalize to s3a://
        test_cases = [
            ("s3://bucket/path/data.parquet", "s3a://bucket/path/data.parquet"),
            ("s3n://bucket/path/data.parquet", "s3a://bucket/path/data.parquet"),
            ("s3a://bucket/path/data.parquet", "s3a://bucket/path/data.parquet"),
        ]

        for input_path, expected_prefix in test_cases:
            protocol, path = parse_spark_filepath(input_path)
            spark_path = to_spark_path(protocol, path)
            assert spark_path.startswith("s3a://"), f"Failed for {input_path}"

    def test_dbfs_path_parsing(self):
        """Test DBFS path parsing."""
        # Test /dbfs/ prefix
        protocol, path = parse_spark_filepath("/dbfs/path/to/data.parquet")
        assert protocol == "dbfs"
        assert path == "/path/to/data.parquet"

        # Test spark path conversion
        spark_path = to_spark_path(protocol, path)
        assert spark_path == "dbfs:/path/to/data.parquet"

        # Test dbfs:/ prefix (single slash)
        protocol2, path2 = parse_spark_filepath("dbfs:/path/to/data.parquet")
        assert protocol2 == "dbfs"
        assert path2 == "/path/to/data.parquet"

        spark_path2 = to_spark_path(protocol2, path2)
        assert spark_path2 == "dbfs:/path/to/data.parquet"

    def test_unity_catalog_path(self):
        """Test Unity Catalog volume paths."""
        filepath = "/Volumes/catalog/schema/volume/data.parquet"
        dataset = SparkDatasetV2(filepath=filepath)

        # Unity Catalog paths should not have file:// prefix
        assert dataset._spark_path == filepath

    def test_gcs_path(self):
        """Test GCS path handling."""
        protocol, path = parse_spark_filepath("gs://bucket/path/data.parquet")
        spark_path = to_spark_path(protocol, path)
        assert spark_path.startswith("gs://")

    def test_azure_path(self):
        """Test Azure Blob Storage path handling."""
        filepath = "abfs://container@account.dfs.core.windows.net/path/data.parquet"
        protocol, path = parse_spark_filepath(filepath)
        spark_path = to_spark_path(protocol, path)
        assert spark_path.startswith("abfs://")


class TestSparkDatasetV2ErrorMessages:
    """Test improved error messages in SparkDatasetV2."""

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_missing_s3fs_error(self, mock_filesystem):
        """Test helpful error for missing s3fs."""
        import_error = ImportError("No module named 's3fs'")
        mock_filesystem.side_effect = import_error

        with pytest.raises(
            ImportError, match="pip install 'kedro-datasets\\[spark-s3\\]'"
        ):
            SparkDatasetV2(filepath="s3://bucket/data.parquet")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_missing_gcsfs_error(self, mock_filesystem):
        """Test helpful error for missing gcsfs."""
        import_error = ImportError("No module named 'gcsfs'")
        mock_filesystem.side_effect = import_error

        with pytest.raises(ImportError, match="pip install.*gcsfs"):
            SparkDatasetV2(filepath="gs://bucket/data.parquet")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_missing_adlfs_error(self, mock_filesystem):
        """Test helpful error for missing adlfs."""
        import_error = ImportError("No module named 'adlfs'")
        mock_filesystem.side_effect = import_error

        with pytest.raises(ImportError, match="pip install.*adlfs"):
            SparkDatasetV2(
                filepath="abfs://container@account.dfs.core.windows.net/data.parquet"
            )


class TestSparkDatasetV2Delta:
    """Test Delta format handling in SparkDatasetV2."""

    @pytest.mark.parametrize("mode", ["merge", "update", "delete"])
    def test_delta_unsupported_modes(self, tmp_path, mode):
        """Test that unsupported Delta modes raise errors."""
        filepath = str(tmp_path / "test.delta")

        with pytest.raises(
            DatasetError,
            match=f"It is not possible to perform 'save\\(\\)' for file format 'delta' with mode '{mode}'",
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
        assert dataset._file_format == "delta"


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

        description = dataset._describe()
        assert "version" in description


class TestSparkDatasetV2Integration:
    """Integration tests for SparkDatasetV2."""

    def test_parallel_runner_restriction(self, tmp_path, sample_spark_df):
        """Test that ParallelRunner is restricted."""
        filepath = str(tmp_path / "test.parquet")
        dataset = SparkDatasetV2(filepath=filepath)
        dataset.save(sample_spark_df)

        DataCatalog({"spark_data": dataset})
        pipeline([node(lambda x: x, "spark_data", "output")])

        # _SINGLE_PROCESS attribute prevents parallel execution
        assert hasattr(dataset, "_SINGLE_PROCESS")
        assert dataset._SINGLE_PROCESS is True

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

    def test_save_args_not_mutated(self, tmp_path, sample_spark_df):
        """Test that save_args dict is not mutated during save."""
        filepath = str(tmp_path / "test.parquet")
        save_args = {"mode": "overwrite", "compression": "snappy"}
        original_save_args = save_args.copy()

        dataset = SparkDatasetV2(filepath=filepath, save_args=save_args)
        dataset.save(sample_spark_df)
        dataset.save(sample_spark_df)  # Save twice to ensure no mutation

        # Original save_args should not be modified
        assert dataset._save_args == original_save_args


# Fixtures for mocking cloud storage
@pytest.fixture
def mock_s3_filesystem():
    """Mock S3 filesystem."""
    with patch("kedro_datasets._utils.spark_utils.fsspec.filesystem") as mock_fs:
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
            credentials={
                "key": "test_key",
                "secret": "test_secret",
            },  # pragma: allowlist secret
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
            credentials={"account_key": "test_key"},  # pragma: allowlist secret
        )

        assert dataset._spark_path.startswith("abfs://")


class TestSparkDatasetV2ExistsErrorHandling:
    """Test _exists method error handling."""

    @patch("kedro_datasets.spark.spark_dataset_v2.get_spark_with_remote_support")
    def test_exists_unexpected_error_raises(self, mock_get_spark, tmp_path):
        """Test that unexpected errors in _exists are re-raised after logging."""
        # Create a dataset
        filepath = str(tmp_path / "test.parquet")

        with patch("kedro_datasets._utils.spark_utils.fsspec.filesystem") as mock_fs:
            mock_filesystem = MagicMock()
            mock_filesystem.exists.return_value = True
            mock_filesystem.glob.return_value = []
            mock_fs.return_value = mock_filesystem

            dataset = SparkDatasetV2(filepath=filepath)

        # Mock Spark to raise an unexpected error (not a "path does not exist" error)
        mock_spark = MagicMock()
        mock_spark.read.format.return_value.load.side_effect = Exception(
            "Unexpected cluster error"
        )
        mock_get_spark.return_value = mock_spark

        # Should re-raise the unexpected error
        with pytest.raises(Exception, match="Unexpected cluster error"):
            dataset._exists()


class TestSparkDatasetV2DBFSOperations:
    """Test DBFS-specific operations."""

    @patch("kedro_datasets.spark.spark_dataset_v2.deployed_on_databricks")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_spark_with_remote_support")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_dbutils")
    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_dbfs_uses_dbutils_when_available(
        self, mock_fsspec, mock_get_dbutils, mock_get_spark, mock_deployed
    ):
        """Test that DBFS paths use dbutils when available on Databricks."""
        # Setup mocks
        mock_deployed.return_value = True
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        mock_dbutils = MagicMock()
        mock_get_dbutils.return_value = mock_dbutils

        # Create dataset with DBFS path
        dataset = SparkDatasetV2(filepath="/dbfs/mnt/data/test.parquet")

        # Verify dbutils was retrieved
        mock_get_dbutils.assert_called_once()

        # The exists and glob functions should be partials using dbutils
        # We can verify this by checking the dataset was created successfully
        assert dataset._protocol == "dbfs"

    @patch("kedro_datasets.spark.spark_dataset_v2.deployed_on_databricks")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_spark_with_remote_support")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_dbutils")
    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_dbfs_falls_back_to_fsspec_when_dbutils_fails(
        self, mock_fsspec, mock_get_dbutils, mock_get_spark, mock_deployed
    ):
        """Test that DBFS falls back to fsspec when dbutils fails."""
        # Setup mocks
        mock_deployed.return_value = True
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        # Make get_dbutils raise an exception
        mock_get_dbutils.side_effect = Exception("dbutils not available")

        # Setup fsspec fallback
        mock_filesystem = MagicMock()
        mock_filesystem.exists.return_value = True
        mock_filesystem.glob.return_value = []
        mock_fsspec.return_value = mock_filesystem

        # Create dataset with DBFS path - should fall back to fsspec
        dataset = SparkDatasetV2(filepath="/dbfs/mnt/data/test.parquet")

        # Verify fsspec was used as fallback
        mock_fsspec.assert_called()
        assert dataset._protocol == "dbfs"

    @patch("kedro_datasets.spark.spark_dataset_v2.deployed_on_databricks")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_spark_with_remote_support")
    @patch("kedro_datasets.spark.spark_dataset_v2.get_dbutils")
    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_dbfs_falls_back_when_dbutils_returns_none(
        self, mock_fsspec, mock_get_dbutils, mock_get_spark, mock_deployed
    ):
        """Test that DBFS falls back to fsspec when dbutils returns None."""
        # Setup mocks
        mock_deployed.return_value = True
        mock_spark = MagicMock()
        mock_get_spark.return_value = mock_spark

        # Make get_dbutils return None
        mock_get_dbutils.return_value = None

        # Setup fsspec fallback
        mock_filesystem = MagicMock()
        mock_filesystem.exists.return_value = True
        mock_filesystem.glob.return_value = []
        mock_fsspec.return_value = mock_filesystem

        # Create dataset with DBFS path - should fall back to fsspec
        dataset = SparkDatasetV2(filepath="/dbfs/mnt/data/test.parquet")

        # Verify fsspec was used as fallback
        mock_fsspec.assert_called()
        assert dataset._protocol == "dbfs"


class TestParseSparkFilepath:
    """Test the parse_spark_filepath utility function."""

    def test_local_absolute_path(self):
        """Test parsing local absolute path."""
        protocol, path = parse_spark_filepath("/home/user/data.parquet")
        assert protocol == "file"
        assert path == "/home/user/data.parquet"

    def test_local_relative_path(self):
        """Test parsing local relative path."""
        protocol, path = parse_spark_filepath("data/test.parquet")
        assert protocol == "file"
        assert path == "data/test.parquet"

    def test_dbfs_with_dbfs_prefix(self):
        """Test parsing /dbfs/ prefixed path."""
        protocol, path = parse_spark_filepath("/dbfs/mnt/data/file.parquet")
        assert protocol == "dbfs"
        assert path == "/mnt/data/file.parquet"

    def test_dbfs_with_scheme(self):
        """Test parsing dbfs:/ scheme path."""
        protocol, path = parse_spark_filepath("dbfs:/mnt/data/file.parquet")
        assert protocol == "dbfs"
        assert path == "/mnt/data/file.parquet"

    def test_unity_catalog_volumes(self):
        """Test parsing Unity Catalog volume path."""
        protocol, path = parse_spark_filepath(
            "/Volumes/catalog/schema/vol/data.parquet"
        )
        assert protocol == "file"
        assert path == "/Volumes/catalog/schema/vol/data.parquet"

    def test_s3_path(self):
        """Test parsing S3 path."""
        protocol, path = parse_spark_filepath("s3://bucket/key/data.parquet")
        assert protocol == "s3"
        assert path == "bucket/key/data.parquet"

    def test_s3a_path(self):
        """Test parsing S3A path."""
        protocol, path = parse_spark_filepath("s3a://bucket/key/data.parquet")
        assert protocol == "s3a"
        assert path == "bucket/key/data.parquet"


class TestToSparkPath:
    """Test the to_spark_path utility function."""

    def test_local_path(self):
        """Test converting local path."""
        spark_path = to_spark_path("file", "/home/user/data.parquet")
        assert spark_path == "file:///home/user/data.parquet"

    def test_unity_catalog_path(self):
        """Test Unity Catalog paths don't get file:// prefix."""
        spark_path = to_spark_path("file", "/Volumes/catalog/schema/vol/data.parquet")
        assert spark_path == "/Volumes/catalog/schema/vol/data.parquet"

    def test_dbfs_path(self):
        """Test DBFS path conversion."""
        spark_path = to_spark_path("dbfs", "/mnt/data/file.parquet")
        assert spark_path == "dbfs:/mnt/data/file.parquet"

    def test_s3_to_s3a(self):
        """Test S3 protocol converts to s3a."""
        spark_path = to_spark_path("s3", "bucket/key/data.parquet")
        assert spark_path == "s3a://bucket/key/data.parquet"

    def test_gcs_path(self):
        """Test GCS path."""
        spark_path = to_spark_path("gs", "bucket/key/data.parquet")
        assert spark_path == "gs://bucket/key/data.parquet"

    def test_azure_path(self):
        """Test Azure path."""
        spark_path = to_spark_path(
            "abfs", "container@account.dfs.core.windows.net/data.parquet"
        )
        assert (
            spark_path == "abfs://container@account.dfs.core.windows.net/data.parquet"
        )
