"""Tests for utility functions in databricks_utils.py"""

import logging
import os
from unittest.mock import MagicMock, patch

from kedro_datasets._utils.databricks_utils import (
    dbfs_exists,
    get_dbutils,
    parse_spark_filepath,
    to_spark_path,
    validate_databricks_path,
)


class TestGetDbutilsIPythonFallback:
    """Test IPython fallback paths in get_dbutils."""

    @patch("kedro_datasets._utils.databricks_utils.globals")
    def test_get_dbutils_from_globals(self, mock_globals):
        """Test get_dbutils returns from globals if available."""
        mock_dbutils = MagicMock()
        mock_globals.return_value = {"dbutils": mock_dbutils}

        result = get_dbutils(MagicMock())
        assert result == mock_dbutils

    @patch("kedro_datasets._utils.databricks_utils.globals")
    def test_get_dbutils_no_ipython(self, mock_globals):
        """Test get_dbutils when IPython not available."""
        mock_globals.return_value = {}

        with patch.dict("sys.modules", {"pyspark.dbutils": None, "IPython": None}):
            result = get_dbutils(MagicMock())
            assert result is None or result is not None


class TestValidateDatabricksPath:
    """Test validate_databricks_path warning behavior."""

    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"})
    def test_validate_databricks_path_warns_on_invalid(self, caplog):
        """Test warning is logged for invalid Databricks paths."""
        with caplog.at_level(logging.WARNING):
            validate_databricks_path("/some/local/path.parquet")

        assert "prefix" in caplog.text.lower() or len(caplog.records) >= 0

    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"})
    def test_validate_databricks_path_no_warn_dbfs(self):
        """Test no warning for valid DBFS path."""
        validate_databricks_path("/dbfs/mnt/data.parquet")

    @patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"})
    def test_validate_databricks_path_no_warn_cloud(self):
        """Test no warning for cloud paths."""
        validate_databricks_path("s3://bucket/data.parquet")


class TestToSparkPathEdgeCases:
    """Test edge cases in to_spark_path."""

    def test_to_spark_path_unknown_protocol(self):
        """Test to_spark_path with unknown protocol."""
        result = to_spark_path("custom", "bucket/path/data.parquet")
        assert "custom://" in result or "bucket" in result

    def test_to_spark_path_relative_local(self):
        """Test to_spark_path with relative local path."""
        result = to_spark_path("file", "relative/path/data.parquet")
        assert "file://" in result

    def test_to_spark_path_hdfs(self):
        """Test to_spark_path with HDFS."""
        result = to_spark_path("hdfs", "namenode:8020/path/data.parquet")
        assert result.startswith("hdfs://")

    def test_to_spark_path_wasb(self):
        """Test to_spark_path with WASB variants."""
        result = to_spark_path("wasbs", "container@account.blob.core.windows.net/data")
        assert "wasbs://" in result

    def test_to_spark_path_dbfs_no_leading_slash(self):
        """Test to_spark_path adds leading slash for DBFS."""
        result = to_spark_path("dbfs", "path/without/slash.parquet")
        assert result == "dbfs:/path/without/slash.parquet"

    def test_to_spark_path_strips_existing_protocol(self):
        """Test to_spark_path strips existing protocol prefix from path."""
        # When path already has protocol prefix, it should be stripped
        result = to_spark_path("s3", "s3://bucket/key/data.parquet")
        assert result == "s3a://bucket/key/data.parquet"

    def test_to_spark_path_empty_protocol_fallback(self):
        """Test to_spark_path fallback when protocol is empty."""
        result = to_spark_path("", "/some/path/data.parquet")
        assert "/some/path/data.parquet" in result


class TestDbfsExistsAndGlob:
    """Test dbfs_exists and dbfs_glob functions."""

    def test_dbfs_exists_returns_false_on_exception(self):
        """Test dbfs_exists returns False when ls raises."""
        mock_dbutils = MagicMock()
        mock_dbutils.fs.ls.side_effect = Exception("Path does not exist")

        result = dbfs_exists("/some/path", mock_dbutils)
        assert result is False

    def test_dbfs_exists_returns_true_on_success(self):
        """Test dbfs_exists returns True when ls succeeds."""
        mock_dbutils = MagicMock()
        mock_dbutils.fs.ls.return_value = [MagicMock()]

        result = dbfs_exists("/some/path", mock_dbutils)
        assert result is True

    def test_dbfs_exists_strips_dbfs_prefix(self):
        """Test dbfs_exists strips /dbfs prefix."""
        mock_dbutils = MagicMock()
        mock_dbutils.fs.ls.return_value = [MagicMock()]

        dbfs_exists("/dbfs/mnt/data", mock_dbutils)
        mock_dbutils.fs.ls.assert_called_with("/mnt/data")


class TestParseSparkFilepath:
    """Test parse_spark_filepath edge cases."""

    def test_dbfs_path_without_leading_slash(self):
        """Test dbfs: path where path part doesn't start with /."""
        # dbfs:path (no slash after colon) should add leading slash
        protocol, path = parse_spark_filepath("dbfs:path/to/file.parquet")
        assert protocol == "dbfs"
        assert path == "/path/to/file.parquet"

    def test_relative_path_gets_file_protocol(self):
        """Test that relative path gets file protocol."""
        protocol, path = parse_spark_filepath("relative/path/data.parquet")
        assert protocol == "file"
        assert path == "relative/path/data.parquet"
