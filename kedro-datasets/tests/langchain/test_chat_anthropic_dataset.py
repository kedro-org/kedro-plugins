"""Tests for ChatAnthropic dataset classes."""

from unittest.mock import Mock, patch

import pytest
from kedro.io import DatasetError

from kedro_datasets.langchain.chat_anthropic_dataset import ChatAnthropicDataset


@pytest.fixture
def anthropic_credentials():
    """Fixture for standard Anthropic credentials."""
    return {
        "base_url": "https://api.anthropic.com/v1",
        "api_key": "sk-ant-test-key",  # pragma: allowlist-secret
    }


@pytest.fixture
def anthropic_kwargs():
    """Fixture for ChatAnthropic kwargs."""
    return {"model": "claude-instant-1", "temperature": 0.0}


class TestAnthropicDataset:
    """Test the ChatAnthropicDataset class."""

    def test_init_with_credentials(self, anthropic_credentials, anthropic_kwargs):
        """Test dataset initialization with credentials."""
        dataset = ChatAnthropicDataset(
            credentials=anthropic_credentials, kwargs=anthropic_kwargs
        )

        assert dataset.credentials == anthropic_credentials
        assert dataset.kwargs == anthropic_kwargs

    def test_init_without_credentials(self):
        """Test dataset initialization without credentials (uses environment variables)."""
        dataset = ChatAnthropicDataset()

        assert dataset.credentials == {}
        assert dataset.kwargs == {}

    def test_init_with_partial_credentials(self):
        """Test dataset initialization with partial credentials works."""
        credentials = {"api_key": "sk-ant-test-key"}  # pragma: allowlist-secret
        dataset = ChatAnthropicDataset(credentials=credentials)

        assert dataset.credentials == credentials
        assert dataset.kwargs == {}

    def test_describe(self, anthropic_credentials, anthropic_kwargs):
        """Test the _describe method returns kwargs."""
        dataset = ChatAnthropicDataset(
            credentials=anthropic_credentials, kwargs=anthropic_kwargs
        )
        description = dataset._describe()
        assert description == {
            **{k: "***" for k in anthropic_credentials.keys()},
            **anthropic_kwargs,
        }

    def test_save_raises_error(self, anthropic_credentials):
        """Test that save method raises DatasetError."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials)
        with pytest.raises(
            DatasetError, match="ChatAnthropicDataset is a read only dataset type"
        ):
            dataset.save(data="test")

    @patch("kedro_datasets.langchain.chat_anthropic_dataset.ChatAnthropic")
    def test_load_with_credentials(
        self, mock_chat_anthropic, anthropic_credentials, anthropic_kwargs
    ):
        """Test that load method creates ChatAnthropic instance with credentials."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance

        dataset = ChatAnthropicDataset(
            credentials=anthropic_credentials, kwargs=anthropic_kwargs
        )
        result = dataset.load()

        mock_chat_anthropic.assert_called_once_with(
            api_key="sk-ant-test-key",  # pragma: allowlist-secret
            base_url="https://api.anthropic.com/v1",
            model="claude-instant-1",
            temperature=0.0,
        )
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_anthropic_dataset.ChatAnthropic")
    def test_load_without_credentials(self, mock_chat_anthropic):
        """Test that load method works without credentials (uses environment variables)."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance

        dataset = ChatAnthropicDataset()
        result = dataset.load()

        mock_chat_anthropic.assert_called_once_with()
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_anthropic_dataset.ChatAnthropic")
    def test_load_with_partial_credentials_api_key_only(self, mock_chat_anthropic):
        """Test that providing only api_key works (url falls back to env)."""
        credentials = {"api_key": "sk-ant-test-key"}  # pragma: allowlist-secret
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance

        dataset = ChatAnthropicDataset(credentials=credentials)
        result = dataset.load()

        mock_chat_anthropic.assert_called_once_with(
            api_key="sk-ant-test-key"  # pragma: allowlist-secret
        )
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_anthropic_dataset.ChatAnthropic")
    def test_load_with_partial_credentials_api_url_only(self, mock_chat_anthropic):
        """Test that providing only api_url works (key falls back to env)."""
        credentials = {"base_url": "https://custom.anthropic.com/v1"}
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance

        dataset = ChatAnthropicDataset(credentials=credentials)
        result = dataset.load()

        mock_chat_anthropic.assert_called_once_with(
            base_url="https://custom.anthropic.com/v1"
        )
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_anthropic_dataset.ChatAnthropic")
    def test_load_with_complex_kwargs(self, mock_chat_anthropic, anthropic_credentials):
        """Test load method with complex kwargs."""
        kwargs = {
            "model": "claude-3-opus-20240229",
            "temperature": 0.1,
            "max_tokens": 4000,
            "top_p": 0.95,
            "stop_sequences": ["Human:", "Assistant:"],
        }
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance

        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=kwargs)
        result = dataset.load()

        mock_chat_anthropic.assert_called_once_with(
            api_key="sk-ant-test-key",  # pragma: allowlist-secret
            base_url="https://api.anthropic.com/v1",
            **kwargs
        )
        assert result == mock_instance
