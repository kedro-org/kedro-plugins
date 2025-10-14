"""Unit tests for LangfuseTraceDataset."""

import os
from unittest.mock import MagicMock

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.langfuse import LangfuseTraceDataset


class TestLangfuseTraceDataset:
    def test_missing_credentials(self):
        """Test that dataset raises error when credentials are missing."""
        with pytest.raises(DatasetError, match="Missing required Langfuse credential"):
            LangfuseTraceDataset(credentials={})

    def test_empty_credentials(self):
        """Test that dataset raises error when credentials are empty."""
        with pytest.raises(DatasetError, match="cannot be empty"):
            LangfuseTraceDataset(credentials={"public_key": "", "secret_key": "sk"})  # pragma: allowlist secret

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
            credentials={"public_key": "pk_test", "secret_key": "sk_test"},  # pragma: allowlist secret
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
                "secret_key": "sk_test", # pragma: allowlist secret
                "host": "https://custom.langfuse.com"
            }
        )

        assert os.environ["LANGFUSE_HOST"] == "https://custom.langfuse.com"

    def test_sdk_mode(self, mocker):
        """Test SDK mode returns client using get_client."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Mock get_client function
        mock_get_client = MagicMock()
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance

        # Create mock langfuse module with get_client
        mock_langfuse_module = MagicMock()
        mock_langfuse_module.get_client = mock_get_client

        mocker.patch.dict("sys.modules", {"langfuse": mock_langfuse_module})

        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}, # pragma: allowlist secret
            mode="sdk"
        )

        result = dataset.load()
        mock_get_client.assert_called_once()
        assert result == mock_client_instance

    def test_sdk_mode_fallback(self, mocker):
        """Test SDK mode falls back to Langfuse() when get_client not available."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Create mock langfuse module without get_client (simulate ImportError)
        mock_langfuse_module = MagicMock()
        mock_langfuse_class = MagicMock()
        mock_langfuse_instance = MagicMock()
        mock_langfuse_class.return_value = mock_langfuse_instance

        # Remove get_client attribute to simulate ImportError
        del mock_langfuse_module.get_client
        mock_langfuse_module.Langfuse = mock_langfuse_class

        mocker.patch.dict("sys.modules", {"langfuse": mock_langfuse_module})

        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}, # pragma: allowlist secret
            mode="sdk"
        )

        result = dataset.load()
        mock_langfuse_class.assert_called_once()
        assert result == mock_langfuse_instance

    def test_load_caching(self, mocker):
        """Test that load() caches the client."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Mock get_client function
        mock_get_client = MagicMock()
        mock_client_instance = MagicMock()
        mock_get_client.return_value = mock_client_instance

        # Create mock langfuse module with get_client
        mock_langfuse_module = MagicMock()
        mock_langfuse_module.get_client = mock_get_client

        mocker.patch.dict("sys.modules", {"langfuse": mock_langfuse_module})

        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}, # pragma: allowlist secret
            mode="sdk"
        )

        # Call load twice
        result1 = dataset.load()
        result2 = dataset.load()

        # Should only create client once due to caching
        mock_get_client.assert_called_once()
        assert result1 is result2  # Same instance

    def test_save_not_implemented(self):
        """Test save raises DatasetError (wrapping NotImplementedError)."""
        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"} # pragma: allowlist secret
        )

        # Kedro wraps NotImplementedError in DatasetError
        with pytest.raises(DatasetError, match="LangfuseTraceDataset is read-only"):
            dataset.save("some_data")

    def test_openai_mode(self, mocker):
        """Test OpenAI mode returns wrapped client."""
        mocker.patch.dict("os.environ", {}, clear=True)

        # Create mock OpenAI module
        mock_openai_module = MagicMock()
        mock_openai_class = MagicMock()
        mock_openai_instance = MagicMock()
        mock_openai_class.return_value = mock_openai_instance
        mock_openai_module.OpenAI = mock_openai_class

        mocker.patch.dict("sys.modules", {"langfuse.openai": mock_openai_module})

        dataset = LangfuseTraceDataset(
            credentials={
                "public_key": "pk_test",
                "secret_key": "sk_test", # pragma: allowlist secret
                "openai": {"openai_api_key": "sk-test"}  # pragma: allowlist secret
            },
            mode="openai"
        )

        result = dataset.load()
        mock_openai_class.assert_called_once_with(api_key="sk-test") # pragma: allowlist secret
        assert result == mock_openai_instance

    def test_openai_mode_missing_credentials(self):
        """Test OpenAI mode raises error when OpenAI credentials missing."""
        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}, # pragma: allowlist secret
            mode="openai"
        )

        with pytest.raises(DatasetError, match="OpenAI mode requires 'openai' section"):
            dataset.load()

    def test_describe_method(self):
        """Test _describe returns correct format."""
        dataset = LangfuseTraceDataset(
            credentials={"public_key": "pk_test", "secret_key": "sk_test"}, # pragma: allowlist secret
            mode="langchain"
        )

        description = dataset._describe()
        assert description == {"mode": "langchain", "credentials": "***"}
