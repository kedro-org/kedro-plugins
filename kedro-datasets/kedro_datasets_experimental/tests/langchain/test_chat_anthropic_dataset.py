"""Tests for ChatAnthropic dataset classes."""

from unittest.mock import Mock, patch

import pytest
from kedro.io import DatasetError

from kedro_datasets_experimental.langchain._anthropic import ChatAnthropicDataset


@pytest.fixture
def anthropic_credentials():
    """Fixture for standard Anthropic credentials."""
    return {
        "anthropic_api_url": "https://api.anthropic.com/v1",
        "anthropic_api_key": "sk-ant-test-key"
    }


@pytest.fixture
def custom_anthropic_credentials():
    """Fixture for custom Anthropic credentials."""
    return {
        "anthropic_api_url": "https://custom.anthropic.com/v1",
        "anthropic_api_key": "sk-ant-custom-key"
    }


@pytest.fixture
def anthropic_kwargs():
    """Fixture for ChatAnthropic kwargs."""
    return {"model": "claude-instant-1", "temperature": 0.0}


@pytest.fixture
def complex_anthropic_kwargs():
    """Fixture for complex ChatAnthropic kwargs."""
    return {
        "model": "claude-3-opus-20240229",
        "temperature": 0.7,
        "max_tokens": 1000,
        "top_p": 0.9
    }


@pytest.fixture
def anthropic_dataset(anthropic_credentials, anthropic_kwargs):
    """Fixture for ChatAnthropicDataset instance."""
    return ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=anthropic_kwargs)


class TestChatAnthropicDataset:
    """Test the ChatAnthropicDataset class."""

    def test_init_with_valid_credentials(self, anthropic_credentials, anthropic_kwargs):
        """Test dataset initialization with valid credentials."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=anthropic_kwargs)
        
        assert dataset.anthropic_api_url == "https://api.anthropic.com/v1"
        assert dataset.anthropic_api_key == "sk-ant-test-key"
        assert dataset.kwargs == anthropic_kwargs

    def test_init_with_missing_api_url(self):
        """Test dataset initialization with missing anthropic_api_url raises KeyError."""
        credentials = {"anthropic_api_key": "sk-ant-test-key"}  # missing anthropic_api_url
        
        with pytest.raises(KeyError, match="anthropic_api_url"):
            ChatAnthropicDataset(credentials=credentials)

    def test_init_with_missing_api_key(self):
        """Test dataset initialization with missing anthropic_api_key raises KeyError."""
        credentials = {"anthropic_api_url": "https://api.anthropic.com/v1"}  # missing anthropic_api_key
        
        with pytest.raises(KeyError, match="anthropic_api_key"):
            ChatAnthropicDataset(credentials=credentials)

    def test_init_with_empty_kwargs(self, anthropic_credentials):
        """Test dataset initialization with None kwargs defaults to empty dict."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=None)
        assert dataset.kwargs == {}

    def test_describe(self, anthropic_dataset):
        """Test the _describe method returns kwargs."""
        description = anthropic_dataset._describe()
        assert description == {"model": "claude-instant-1", "temperature": 0.0}

    def test_save_raises_error(self, anthropic_dataset):
        """Test that save method raises DatasetError."""
        with pytest.raises(DatasetError, match="ChatAnthropicDataset is a read only dataset type"):
            anthropic_dataset.save(data="test")

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_creates_chat_anthropic(self, mock_chat_anthropic, anthropic_dataset):
        """Test that load method creates ChatAnthropic instance."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        result = anthropic_dataset.load()
        
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://api.anthropic.com/v1",
            anthropic_api_key="sk-ant-test-key",
            model="claude-instant-1",
            temperature=0.0
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_with_no_kwargs(self, mock_chat_anthropic, anthropic_credentials):
        """Test load method works without additional kwargs."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials)
        result = dataset.load()
        
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://api.anthropic.com/v1",
            anthropic_api_key="sk-ant-test-key"
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_with_multiple_kwargs(self, mock_chat_anthropic, custom_anthropic_credentials, complex_anthropic_kwargs):
        """Test load method with multiple kwargs."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        dataset = ChatAnthropicDataset(credentials=custom_anthropic_credentials, kwargs=complex_anthropic_kwargs)
        result = dataset.load()
        
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://custom.anthropic.com/v1",
            anthropic_api_key="sk-ant-custom-key",
            model="claude-3-opus-20240229",
            temperature=0.7,
            max_tokens=1000,
            top_p=0.9
        )
        assert result == mock_instance

    def test_describe_with_complex_kwargs(self, anthropic_credentials, complex_anthropic_kwargs):
        """Test describe method returns complex kwargs."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=complex_anthropic_kwargs)
        description = dataset._describe()
        
        assert description == complex_anthropic_kwargs

    def test_empty_credentials_dict(self):
        """Test that empty credentials dict raises KeyError."""
        credentials = {}
        
        with pytest.raises(KeyError):
            ChatAnthropicDataset(credentials=credentials)


class TestAnthropicCredentialsValidation:
    """Test credential validation for ChatAnthropicDataset."""

    def test_missing_anthropic_api_url(self):
        """Test that missing anthropic_api_url raises KeyError."""
        credentials = {"anthropic_api_key": "sk-ant-test-key"}
        
        with pytest.raises(KeyError, match="anthropic_api_url"):
            ChatAnthropicDataset(credentials=credentials)

    def test_missing_anthropic_api_key(self):
        """Test that missing anthropic_api_key raises KeyError."""
        credentials = {"anthropic_api_url": "https://api.anthropic.com/v1"}
        
        with pytest.raises(KeyError, match="anthropic_api_key"):
            ChatAnthropicDataset(credentials=credentials)

    def test_both_credentials_missing(self):
        """Test that missing both credentials raises KeyError."""
        credentials = {}
        
        with pytest.raises(KeyError):
            ChatAnthropicDataset(credentials=credentials)


class TestAnthropicIntegration:
    """Integration tests that verify the full workflow without mocking."""

    def test_anthropic_dataset_integration(self, anthropic_dataset):
        """Test ChatAnthropicDataset integration without API calls."""
        # Verify properties without calling load() to avoid API calls
        assert anthropic_dataset.anthropic_api_url == "https://api.anthropic.com/v1"
        assert anthropic_dataset.anthropic_api_key == "sk-ant-test-key"
        assert anthropic_dataset.kwargs == {"model": "claude-instant-1", "temperature": 0.0}
        assert anthropic_dataset._describe() == {"model": "claude-instant-1", "temperature": 0.0}

    def test_credentials_storage(self, anthropic_credentials, anthropic_kwargs):
        """Test that credentials are properly stored in dataset instance."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=anthropic_kwargs)
        
        assert dataset.anthropic_api_url == anthropic_credentials["anthropic_api_url"]
        assert dataset.anthropic_api_key == anthropic_credentials["anthropic_api_key"]


class TestAnthropicEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_special_characters_in_credentials(self):
        """Test dataset works with special characters in credentials."""
        credentials = {
            "anthropic_api_url": "https://api.anthropic.com/v1?param=value&other=test",
            "anthropic_api_key": "sk-ant-key-with-dashes_and_underscores"
        }
        
        dataset = ChatAnthropicDataset(credentials=credentials)
        assert dataset.anthropic_api_url == credentials["anthropic_api_url"]
        assert dataset.anthropic_api_key == credentials["anthropic_api_key"]

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_with_complex_kwargs(self, mock_chat_anthropic, anthropic_credentials):
        """Test load method with complex nested kwargs."""
        kwargs = {
            "model": "claude-3-sonnet-20240229",
            "temperature": 0.3,
            "max_tokens": 2000,
            "stop_sequences": ["END", "STOP"],
            "top_k": 40
        }
        
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=kwargs)
        result = dataset.load()
        
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://api.anthropic.com/v1",
            anthropic_api_key="sk-ant-test-key",
            **kwargs
        )
        assert result == mock_instance

    def test_credentials_immutability(self):
        """Test that modifying external credentials dict doesn't affect dataset."""
        credentials = {
            "anthropic_api_url": "https://api.anthropic.com/v1",
            "anthropic_api_key": "sk-ant-test-key"
        }
        
        dataset = ChatAnthropicDataset(credentials=credentials)
        
        # Modify original credentials
        credentials["anthropic_api_key"] = "modified-key"
        
        # Dataset should still have original credentials
        assert dataset.anthropic_api_key == "sk-ant-test-key"

    def test_empty_string_credentials(self):
        """Test behavior with empty string credentials."""
        credentials = {
            "anthropic_api_url": "",
            "anthropic_api_key": ""
        }
        
        dataset = ChatAnthropicDataset(credentials=credentials)
        assert dataset.anthropic_api_url == ""
        assert dataset.anthropic_api_key == ""

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_preserves_parameter_mapping(self, mock_chat_anthropic, anthropic_credentials):
        """Test that load method correctly passes credentials to ChatAnthropic parameters."""
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials)
        dataset.load()
        
        # Verify the correct parameter passing
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://api.anthropic.com/v1",
            anthropic_api_key="sk-ant-test-key"
        )

    def test_describe_empty_kwargs(self, anthropic_credentials):
        """Test describe method with empty kwargs."""
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials)
        description = dataset._describe()
        
        assert description == {}

    def test_describe_returns_copy_of_kwargs(self, anthropic_credentials):
        """Test that describe returns a copy of kwargs, not reference."""
        kwargs = {"model": "claude-instant-1", "temperature": 0.5}
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=kwargs)
        
        description = dataset._describe()
        description["new_param"] = "new_value"
        
        # Original kwargs should not be modified
        assert "new_param" not in dataset.kwargs
        assert dataset.kwargs == {"model": "claude-instant-1", "temperature": 0.5}

    @patch('kedro_datasets_experimental.langchain._anthropic.ChatAnthropic')
    def test_load_with_anthropic_specific_params(self, mock_chat_anthropic, anthropic_credentials):
        """Test load method with Anthropic-specific parameters."""
        kwargs = {
            "model": "claude-3-opus-20240229",
            "temperature": 0.1,
            "max_tokens": 4000,
            "top_p": 0.95,
            "top_k": 50,
            "stop_sequences": ["Human:", "Assistant:"],
            "stream": False
        }
        
        mock_instance = Mock()
        mock_chat_anthropic.return_value = mock_instance
        
        dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=kwargs)
        result = dataset.load()
        
        mock_chat_anthropic.assert_called_once_with(
            anthropic_api_url="https://api.anthropic.com/v1",
            anthropic_api_key="sk-ant-test-key",
            **kwargs
        )
        assert result == mock_instance

    def test_claude_model_variations(self, anthropic_credentials):
        """Test dataset works with different Claude model names."""
        claude_models = [
            "claude-instant-1",
            "claude-2",
            "claude-3-sonnet-20240229",
            "claude-3-opus-20240229",
            "claude-3-haiku-20240307"
        ]
        
        for model in claude_models:
            kwargs = {"model": model}
            dataset = ChatAnthropicDataset(credentials=anthropic_credentials, kwargs=kwargs)
            assert dataset.kwargs["model"] == model
            assert dataset._describe()["model"] == model
