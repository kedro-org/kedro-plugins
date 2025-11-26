"""Tests for utility functions in spark_utils.py."""

import os
from unittest.mock import MagicMock, patch

import pytest
from kedro.io.core import DatasetError

from kedro_datasets._utils.spark_utils import (
    get_spark_filesystem,
    get_spark_with_remote_support,
    load_spark_schema_from_file,
)


class TestGetSparkWithRemoteSupport:
    """Test various branches of get_spark_with_remote_support."""

    @patch.dict(
        os.environ,
        {"DATABRICKS_HOST": "host.databricks.com", "DATABRICKS_TOKEN": "token123"},
    )
    @patch("pyspark.sql.SparkSession")
    def test_databricks_connect_path(self, mock_spark_session):
        """Test Databricks Connect environment path."""
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.remote.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        get_spark_with_remote_support()
        mock_builder.remote.assert_called_once()

    @patch.dict(os.environ, {"SPARK_REMOTE": "sc://localhost:15002"}, clear=False)
    @patch("pyspark.sql.SparkSession")
    def test_spark_connect_path(self, mock_spark_session):
        """Test Spark Connect environment path."""
        env = os.environ.copy()
        env.pop("DATABRICKS_HOST", None)
        env.pop("DATABRICKS_TOKEN", None)
        env["SPARK_REMOTE"] = "sc://localhost:15002"

        with patch.dict(os.environ, env, clear=True):
            mock_builder = MagicMock()
            mock_spark_session.builder = mock_builder
            mock_builder.remote.return_value = mock_builder
            mock_builder.getOrCreate.return_value = MagicMock()

            get_spark_with_remote_support()

    @patch.dict(os.environ, {}, clear=False)
    @patch("pyspark.sql.SparkSession")
    def test_classic_spark_fallback(self, mock_spark_session):
        """Test classic Spark session fallback."""
        env = {
            k: v
            for k, v in os.environ.items()
            if k not in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "SPARK_REMOTE")
        }

        with patch.dict(os.environ, env, clear=True):
            mock_builder = MagicMock()
            mock_spark_session.builder = mock_builder
            mock_builder.getOrCreate.return_value = MagicMock()

            get_spark_with_remote_support()
            mock_builder.getOrCreate.assert_called()

    @patch.dict(os.environ, {"DATABRICKS_HOST": "host", "DATABRICKS_TOKEN": "token"})
    @patch("pyspark.sql.SparkSession")
    def test_databricks_connect_fails_falls_back(self, mock_spark_session):
        """Test fallback when Databricks Connect fails."""
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.remote.side_effect = Exception("Connection failed")
        mock_builder.getOrCreate.return_value = MagicMock()

        get_spark_with_remote_support()

    @patch("pyspark.sql.SparkSession")
    def test_spark_connect_fails_falls_back_to_classic(self, mock_spark_session):
        """Test fallback to classic Spark when Spark Connect fails."""
        env = {"SPARK_REMOTE": "sc://localhost:15002"}

        with patch.dict(os.environ, env, clear=True):
            mock_builder = MagicMock()
            mock_spark_session.builder = mock_builder
            # First remote() call fails
            mock_builder.remote.side_effect = Exception("Spark Connect failed")
            mock_builder.getOrCreate.return_value = MagicMock()

            get_spark_with_remote_support()
            # Should still call getOrCreate after remote fails
            mock_builder.getOrCreate.assert_called()


class TestGetSparkFilesystemErrors:
    """Test error message paths in get_spark_filesystem."""

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_s3fs_import_error(self, mock_fs):
        """Test S3 import error message."""
        mock_fs.side_effect = ImportError("No module named 's3fs'")

        with pytest.raises(ImportError, match="s3fs.*not installed"):
            get_spark_filesystem("s3")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_gcsfs_import_error(self, mock_fs):
        """Test GCS import error message."""
        mock_fs.side_effect = ImportError("No module named 'gcsfs'")

        with pytest.raises(ImportError, match="gcsfs"):
            get_spark_filesystem("gcs")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_adlfs_import_error(self, mock_fs):
        """Test Azure import error message."""
        mock_fs.side_effect = ImportError("No module named 'adlfs'")

        with pytest.raises(ImportError, match="adlfs"):
            get_spark_filesystem("abfs")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_hdfs_import_error(self, mock_fs):
        """Test HDFS import error message."""
        mock_fs.side_effect = ImportError("No module named 'pyarrow'")

        with pytest.raises(ImportError, match="PyArrow|HDFS"):
            get_spark_filesystem("hdfs")

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_generic_import_error(self, mock_fs):
        """Test generic import error message."""
        mock_fs.side_effect = ImportError("Unknown module")

        with pytest.raises(ImportError, match="Missing filesystem"):
            get_spark_filesystem("someprotocol")


class TestLoadSparkSchemaErrors:
    """Test schema loading error paths."""

    def test_schema_missing_filepath_key(self):
        """Test error when schema config missing filepath."""
        with pytest.raises(DatasetError, match="does not specify a 'filepath'"):
            load_spark_schema_from_file({})

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_schema_invalid_json(self, mock_fs):
        """Test error when schema file has invalid JSON."""
        mock_filesystem = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = "not valid json {"
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_filesystem.open.return_value = mock_file
        mock_fs.return_value = mock_filesystem

        with pytest.raises(DatasetError, match="invalid JSON"):
            load_spark_schema_from_file({"filepath": "/path/to/schema.json"})

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    @patch("pyspark.sql.types.StructType")
    def test_schema_invalid_structtype(self, mock_struct, mock_fs):
        """Test error when schema file has invalid StructType."""
        mock_filesystem = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = '{"valid": "json"}'
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_filesystem.open.return_value = mock_file
        mock_fs.return_value = mock_filesystem

        mock_struct.fromJson.side_effect = Exception("Invalid schema")

        with pytest.raises(DatasetError, match="invalid"):
            load_spark_schema_from_file({"filepath": "/path/to/schema.json"})

    @patch("kedro_datasets._utils.spark_utils.fsspec.filesystem")
    def test_schema_relative_path_uses_file_protocol(self, mock_fs):
        """Test that relative schema path defaults to file protocol."""
        mock_filesystem = MagicMock()
        mock_file = MagicMock()
        mock_file.read.return_value = "not valid json {"
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)
        mock_filesystem.open.return_value = mock_file
        mock_fs.return_value = mock_filesystem

        # Use relative path (no protocol) - should default to "file"
        with pytest.raises(DatasetError):
            load_spark_schema_from_file({"filepath": "relative/schema.json"})

        # Verify fsspec was called with "file" protocol
        mock_fs.assert_called_once_with("file")
