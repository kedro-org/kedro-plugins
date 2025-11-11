"""Tests for ChatCohere dataset classes."""

from unittest.mock import Mock, patch

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.langchain._cohere import ChatCohereDataset


@pytest.fixture
def cohere_credentials():
    """Fixture for standard Cohere credentials."""
    return {
        "base_url": "https://api.cohere.ai/v1",
        "api_key": "test-cohere-key" # pragma: allowlist-secret
    }


@pytest.fixture
def cohere_kwargs():
    """Fixture for ChatCohere kwargs."""
    return {"model": "command", "temperature": 0.0}


class TestCohereDataset:
    """Test the ChatCohereDataset class."""

    def test_init_with_credentials(self, cohere_credentials, cohere_kwargs):
        """Test dataset initialization with credentials."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)

        assert dataset.credentials == cohere_credentials
        assert dataset.kwargs == cohere_kwargs

    def test_init_without_credentials(self):
        """Test dataset initialization without credentials (uses environment variables)."""
        dataset = ChatCohereDataset()

        assert dataset.credentials == {}
        assert dataset.kwargs == {}

    def test_init_with_partial_credentials(self):
        """Test dataset initialization with partial credentials works."""
        credentials = {"api_key": "test-cohere-key"}  # pragma: allowlist-secret
        dataset = ChatCohereDataset(credentials=credentials)

        assert dataset.credentials == credentials
        assert dataset.kwargs == {}

    def test_describe(self, cohere_credentials, cohere_kwargs):
        """Test the _describe method returns kwargs."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)
        description = dataset._describe()
        assert description == cohere_kwargs

    def test_save_raises_error(self, cohere_credentials):
        """Test that save method raises DatasetError."""
        dataset = ChatCohereDataset(credentials=cohere_credentials)
        with pytest.raises(DatasetError, match="ChatCohereDataset is a read only dataset type"):
            dataset.save(data="test")

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_credentials(self, mock_chat_cohere, cohere_credentials, cohere_kwargs):
        """Test that load method creates ChatCohere instance with credentials."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(
            api_key="test-cohere-key",   # pragma: allowlist-secret
            base_url="https://api.cohere.ai/v1",
            model="command",
            temperature=0.0
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_without_credentials(self, mock_chat_cohere):
        """Test that load method works without credentials (uses environment variables)."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset()
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with()
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_partial_credentials_api_key_only(self, mock_chat_cohere):
        """Test that providing only api_key works (url falls back to env)."""
        credentials = {"api_key": "test-cohere-key"}  # pragma: allowlist-secret
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=credentials)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(api_key="test-cohere-key")  # pragma: allowlist-secret
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_partial_credentials_api_url_only(self, mock_chat_cohere):
        """Test that providing only api_url works (key falls back to env)."""
        credentials = {"base_url": "https://custom.cohere.ai/v1"}
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=credentials)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(base_url="https://custom.cohere.ai/v1")
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_complex_kwargs(self, mock_chat_cohere, cohere_credentials):
        """Test load method with complex kwargs."""
        kwargs = {
            "model": "command-r",
            "temperature": 0.5,
            "max_tokens": 500,
            "top_p": 0.9,
            "frequency_penalty": 0.1
        }
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=kwargs)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(
            api_key="test-cohere-key",   # pragma: allowlist-secret
            base_url="https://api.cohere.ai/v1",
            **kwargs
        )
        assert result == mock_instance
