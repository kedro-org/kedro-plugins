"""Unit tests for LangfuseTraceDataset."""

import os
from unittest.mock import MagicMock

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.langfuse import LangfuseTraceDataset


class TestLangfuseTraceDataset:
    def test_missing_credentials(self):
        """Test that dataset raises error when credentials are missing."""
        with pytest.raises(ValueError, match="Missing required Langfuse credential"):
            LangfuseTraceDataset(credentials={})

    def test_empty_credentials(self):
        """Test that dataset raises error when credentials are empty."""
        with pytest.raises(ValueError, match="cannot be empty"):
            LangfuseTraceDataset(credentials={"public_key": "", "secret_key": "sk"})

    def test_langchain_mode(self, mocker):
        """Test langchain mode returns CallbackHandler."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Create a mock module structure
        mock_langchain = MagicMock()
        mock_handler = MagicMock()
        mock_langchain.CallbackHandler = mock_handler

        # Mock the langfuse.langchain module
        mocker.patch.dict("sys.modules", {"langfuse.langchain": mock_langchain})

        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"},
            mode="langchain"
        )

        result = dataset.load()
        mock_handler.assert_called_once()
        assert result == mock_handler.return_value

    def test_host_setting(self, mocker):
        """Test that host is set in environment when provided."""
        mocker.patch.dict("os.environ", {}, clear=True)

        LangfuseTraceDataset(
            credentials={
                "public_key": "pk_test",
                "secret_key": "sk_test",
                "host": "https://custom.langfuse.com"
            }
        )

        assert os.environ["LANGFUSE_HOST"] == "https://custom.langfuse.com"

    def test_sdk_mode(self, mocker):
        """Test SDK mode returns Langfuse client."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Mock at module level since Langfuse is imported at the top
        mock_langfuse_class = MagicMock()
        mocker.patch("kedro_datasets_experimental.langfuse.langfuse_trace_dataset.Langfuse", mock_langfuse_class)

        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"},
            mode="sdk"
        )

        result = dataset.load()
        mock_langfuse_class.assert_called_once()
        assert result == mock_langfuse_class.return_value

    def test_save_not_implemented(self):
        """Test save raises DatasetError (wrapping NotImplementedError)."""
        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}
        )

        # Kedro wraps NotImplementedError in DatasetError
        with pytest.raises(DatasetError, match="LangfuseTraceDataset is read-only"):
            dataset.save("some_data")
