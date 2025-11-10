"""Tests for ChatCohere dataset classes."""

from unittest.mock import Mock, patch

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.langchain._cohere import ChatCohereDataset


@pytest.fixture
def cohere_credentials():
    """Fixture for standard Cohere credentials."""
    return {
        "cohere_api_url": "https://api.cohere.ai/v1",
        "cohere_api_key": "test-cohere-key" # pragma: allowlist-secret
    }


@pytest.fixture
def custom_cohere_credentials():
    """Fixture for custom Cohere credentials."""
    return {
        "cohere_api_url": "https://custom.cohere.ai/v1",
        "cohere_api_key": "custom-cohere-key"   # pragma: allowlist-secret
    }


@pytest.fixture
def cohere_kwargs():
    """Fixture for ChatCohere kwargs."""
    return {"model": "command", "temperature": 0.0}


@pytest.fixture
def complex_cohere_kwargs():
    """Fixture for complex ChatCohere kwargs."""
    return {
        "model": "command-r",
        "temperature": 0.5,
        "max_tokens": 500,
        "top_p": 0.9
    }


@pytest.fixture
def cohere_dataset(cohere_credentials, cohere_kwargs):
    """Fixture for ChatCohereDataset instance."""
    return ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)


class TestChatCohereDataset:
    """Test the ChatCohereDataset class."""

    def test_init_with_valid_credentials(self, cohere_credentials, cohere_kwargs):
        """Test dataset initialization with valid credentials."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)

        assert dataset.cohere_api_url == "https://api.cohere.ai/v1"
        assert dataset.cohere_api_key == "test-cohere-key"  # pragma: allowlist-secret
        assert dataset.kwargs == cohere_kwargs

    def test_init_with_missing_api_url(self):
        """Test dataset initialization with missing cohere_api_url raises KeyError."""
        credentials = {"cohere_api_key": "test-cohere-key"}  # missing cohere_api_url   # pragma: allowlist-secret

        with pytest.raises(KeyError, match="cohere_api_url"):
            ChatCohereDataset(credentials=credentials)

    def test_init_with_missing_api_key(self):
        """Test dataset initialization with missing cohere_api_key raises KeyError."""
        credentials = {"cohere_api_url": "https://api.cohere.ai/v1"}  # missing cohere_api_key

        with pytest.raises(KeyError, match="cohere_api_key"):
            ChatCohereDataset(credentials=credentials)

    def test_init_with_empty_kwargs(self, cohere_credentials):
        """Test dataset initialization with None kwargs defaults to empty dict."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=None)
        assert dataset.kwargs == {}

    def test_describe(self, cohere_dataset):
        """Test the _describe method returns kwargs."""
        description = cohere_dataset._describe()
        assert description == {"model": "command", "temperature": 0.0}

    def test_save_raises_error(self, cohere_dataset):
        """Test that save method raises DatasetError."""
        with pytest.raises(DatasetError, match="ChatCohereDataset is a read only dataset type"):
            cohere_dataset.save(data="test")

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_creates_chat_cohere(self, mock_chat_cohere, cohere_dataset):
        """Test that load method creates ChatCohere instance."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        result = cohere_dataset.load()

        mock_chat_cohere.assert_called_once_with(
            cohere_api_key="test-cohere-key",   # pragma: allowlist-secret
            base_url="https://api.cohere.ai/v1",
            model="command",
            temperature=0.0
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_no_kwargs(self, mock_chat_cohere, cohere_credentials):
        """Test load method works without additional kwargs."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=cohere_credentials)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(
            cohere_api_key="test-cohere-key",   # pragma: allowlist-secret
            base_url="https://api.cohere.ai/v1"
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_with_multiple_kwargs(self, mock_chat_cohere, custom_cohere_credentials, complex_cohere_kwargs):
        """Test load method with multiple kwargs."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=custom_cohere_credentials, kwargs=complex_cohere_kwargs)
        result = dataset.load()

        mock_chat_cohere.assert_called_once_with(
            cohere_api_key="custom-cohere-key", # pragma: allowlist-secret
            base_url="https://custom.cohere.ai/v1",
            model="command-r",
            temperature=0.5,
            max_tokens=500,
            top_p=0.9
        )
        assert result == mock_instance

    def test_describe_with_complex_kwargs(self, cohere_credentials, complex_cohere_kwargs):
        """Test describe method returns complex kwargs."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=complex_cohere_kwargs)
        description = dataset._describe()

        assert description == complex_cohere_kwargs

    def test_empty_credentials_dict(self):
        """Test that empty credentials dict raises KeyError."""
        credentials = {}

        with pytest.raises(KeyError):
            ChatCohereDataset(credentials=credentials)


class TestCohereCredentialsValidation:
    """Test credential validation for ChatCohereDataset."""

    def test_missing_cohere_api_url(self):
        """Test that missing cohere_api_url raises KeyError."""
        credentials = {"cohere_api_key": "test-cohere-key"} # pragma: allowlist-secret

        with pytest.raises(KeyError, match="cohere_api_url"):
            ChatCohereDataset(credentials=credentials)

    def test_missing_cohere_api_key(self):
        """Test that missing cohere_api_key raises KeyError."""
        credentials = {"cohere_api_url": "https://api.cohere.ai/v1"}

        with pytest.raises(KeyError, match="cohere_api_key"):
            ChatCohereDataset(credentials=credentials)

    def test_both_credentials_missing(self):
        """Test that missing both credentials raises KeyError."""
        credentials = {}

        with pytest.raises(KeyError):
            ChatCohereDataset(credentials=credentials)


class TestCohereIntegration:
    """Integration tests that verify the full workflow without mocking."""

    def test_cohere_dataset_integration(self, cohere_dataset):
        """Test ChatCohereDataset integration without API calls."""
        # Verify properties without calling load() to avoid API calls
        assert cohere_dataset.cohere_api_url == "https://api.cohere.ai/v1"
        assert cohere_dataset.cohere_api_key == "test-cohere-key"   # pragma: allowlist-secret
        assert cohere_dataset.kwargs == {"model": "command", "temperature": 0.0}
        assert cohere_dataset._describe() == {"model": "command", "temperature": 0.0}

    def test_credentials_storage(self, cohere_credentials, cohere_kwargs):
        """Test that credentials are properly stored in dataset instance."""
        dataset = ChatCohereDataset(credentials=cohere_credentials, kwargs=cohere_kwargs)

        assert dataset.cohere_api_url == cohere_credentials["cohere_api_url"]
        assert dataset.cohere_api_key == cohere_credentials["cohere_api_key"]

    @patch('kedro_datasets_experimental.langchain._cohere.ChatCohere')
    def test_load_preserves_parameter_mapping(self, mock_chat_cohere, cohere_credentials):
        """Test that load method correctly maps credentials to ChatCohere parameters."""
        mock_instance = Mock()
        mock_chat_cohere.return_value = mock_instance

        dataset = ChatCohereDataset(credentials=cohere_credentials)
        dataset.load()

        # Verify the correct parameter mapping
        mock_chat_cohere.assert_called_once_with(
            cohere_api_key="test-cohere-key",  # mapped from cohere_api_key    # pragma: allowlist-secret
            base_url="https://api.cohere.ai/v1"  # mapped from cohere_api_url to base_url
        )
