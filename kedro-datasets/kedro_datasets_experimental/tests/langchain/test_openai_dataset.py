"""Tests for OpenAI dataset classes."""

from unittest.mock import MagicMock, Mock, patch

import pytest
from kedro.io import DatasetError
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from kedro_datasets_experimental.langchain._openai import (
    ChatOpenAIDataset,
    OpenAIDataset,
    OpenAIEmbeddingsDataset,
)


@pytest.fixture
def openai_credentials():
    """Fixture for standard OpenAI credentials."""
    return {
        "openai_api_base": "https://api.openai.com/v1",
        "openai_api_key": "sk-test-key"
    }


@pytest.fixture
def custom_openai_credentials():
    """Fixture for custom OpenAI credentials."""
    return {
        "openai_api_base": "https://custom.openai.com/v1",
        "openai_api_key": "sk-custom-key"
    }


@pytest.fixture
def chat_kwargs():
    """Fixture for ChatOpenAI kwargs."""
    return {"model": "gpt-3.5-turbo", "temperature": 0.7}


@pytest.fixture
def embeddings_kwargs():
    """Fixture for OpenAIEmbeddings kwargs."""
    return {"model": "text-embedding-ada-002"}


@pytest.fixture
def complex_chat_kwargs():
    """Fixture for complex ChatOpenAI kwargs."""
    return {
        "model": "gpt-4",
        "temperature": 0.0,
        "max_tokens": 100,
        "top_p": 0.9
    }


@pytest.fixture
def test_openai_dataset():
    """Fixture that creates a concrete TestDataset for base class testing."""
    class TestDataset(OpenAIDataset):
        @property
        def constructor(self):
            return Mock
    return TestDataset


@pytest.fixture
def embeddings_dataset(openai_credentials, embeddings_kwargs):
    """Fixture for OpenAIEmbeddingsDataset instance."""
    return OpenAIEmbeddingsDataset(credentials=openai_credentials, kwargs=embeddings_kwargs)


@pytest.fixture
def chat_dataset(openai_credentials, chat_kwargs):
    """Fixture for ChatOpenAIDataset instance."""
    return ChatOpenAIDataset(credentials=openai_credentials, kwargs=chat_kwargs)


class TestOpenAIDataset:
    """Test the base OpenAIDataset class."""

    def test_init_with_valid_credentials(self, test_openai_dataset, openai_credentials, chat_kwargs):
        """Test dataset initialization with valid credentials."""
        dataset = test_openai_dataset(credentials=openai_credentials, kwargs=chat_kwargs)
        
        assert dataset.openai_api_base == "https://api.openai.com/v1"
        assert dataset.openai_api_key == "sk-test-key"
        assert dataset.kwargs == chat_kwargs

    def test_init_with_missing_credentials(self, test_openai_dataset):
        """Test dataset initialization with missing credentials raises KeyError."""
        credentials = {"openai_api_key": "sk-test-key"}  # missing openai_api_base
        
        with pytest.raises(KeyError):
            test_openai_dataset(credentials=credentials)

    def test_init_with_empty_kwargs(self, test_openai_dataset, openai_credentials):
        """Test dataset initialization with None kwargs defaults to empty dict."""
        dataset = test_openai_dataset(credentials=openai_credentials, kwargs=None)
        assert dataset.kwargs == {}

    def test_describe(self, test_openai_dataset, openai_credentials, chat_kwargs):
        """Test the _describe method returns kwargs."""
        dataset = test_openai_dataset(credentials=openai_credentials, kwargs=chat_kwargs)
        description = dataset._describe()
        
        assert description == chat_kwargs

    def test_save_raises_error(self, test_openai_dataset, openai_credentials):
        """Test that save method raises DatasetError."""
        dataset = test_openai_dataset(credentials=openai_credentials)
        
        with pytest.raises(DatasetError, match="TestDataset is a read only dataset type"):
            dataset.save(data="test")

    def test_load_calls_constructor_with_correct_args(self, openai_credentials, chat_kwargs):
        """Test that load method calls constructor with correct arguments."""
        mock_constructor = Mock()
        mock_instance = Mock()
        mock_constructor.return_value = mock_instance
        
        class TestDataset(OpenAIDataset):
            @property
            def constructor(self):
                return mock_constructor
        
        dataset = TestDataset(credentials=openai_credentials, kwargs=chat_kwargs)
        result = dataset.load()
        
        mock_constructor.assert_called_once_with(
            openai_api_base="https://api.openai.com/v1",
            openai_api_key="sk-test-key",
            model="gpt-3.5-turbo",
            temperature=0.7
        )
        assert result == mock_instance


class TestOpenAIEmbeddingsDataset:
    """Test the OpenAIEmbeddingsDataset class."""

    def test_constructor_property(self, embeddings_dataset):
        """Test that constructor property returns OpenAIEmbeddings."""
        assert embeddings_dataset.constructor == OpenAIEmbeddings

    @patch('kedro_datasets_experimental.langchain._openai.OpenAIEmbeddings')
    def test_load_creates_openai_embeddings(self, mock_openai_embeddings, embeddings_dataset):
        """Test that load method creates OpenAIEmbeddings instance."""
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance
        
        result = embeddings_dataset.load()
        
        mock_openai_embeddings.assert_called_once_with(
            openai_api_base="https://api.openai.com/v1",
            openai_api_key="sk-test-key",
            model="text-embedding-ada-002"
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._openai.OpenAIEmbeddings')
    def test_load_with_no_kwargs(self, mock_openai_embeddings, openai_credentials):
        """Test load method works without additional kwargs."""
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance
        
        dataset = OpenAIEmbeddingsDataset(credentials=openai_credentials)
        result = dataset.load()
        
        mock_openai_embeddings.assert_called_once_with(
            openai_api_base="https://api.openai.com/v1",
            openai_api_key="sk-test-key"
        )
        assert result == mock_instance

    def test_describe_with_model_kwargs(self, openai_credentials):
        """Test describe method returns model kwargs."""
        kwargs = {"model": "text-embedding-ada-002", "chunk_size": 1000}
        dataset = OpenAIEmbeddingsDataset(credentials=openai_credentials, kwargs=kwargs)
        description = dataset._describe()
        
        assert description == kwargs

    def test_save_raises_error(self, embeddings_dataset):
        """Test that save method raises appropriate error."""
        with pytest.raises(DatasetError, match="OpenAIEmbeddingsDataset is a read only dataset type"):
            embeddings_dataset.save(data="test")


class TestChatOpenAIDataset:
    """Test the ChatOpenAIDataset class."""

    def test_constructor_property(self, chat_dataset):
        """Test that constructor property returns ChatOpenAI."""
        assert chat_dataset.constructor == ChatOpenAI

    @patch('kedro_datasets_experimental.langchain._openai.ChatOpenAI')
    def test_load_creates_chat_openai(self, mock_chat_openai, chat_dataset):
        """Test that load method creates ChatOpenAI instance."""
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance
        
        result = chat_dataset.load()
        
        mock_chat_openai.assert_called_once_with(
            openai_api_base="https://api.openai.com/v1",
            openai_api_key="sk-test-key",
            model="gpt-3.5-turbo",
            temperature=0.7
        )
        assert result == mock_instance

    @patch('kedro_datasets_experimental.langchain._openai.ChatOpenAI')
    def test_load_with_multiple_kwargs(self, mock_chat_openai, custom_openai_credentials, complex_chat_kwargs):
        """Test load method with multiple kwargs."""
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance
        
        dataset = ChatOpenAIDataset(credentials=custom_openai_credentials, kwargs=complex_chat_kwargs)
        result = dataset.load()
        
        mock_chat_openai.assert_called_once_with(
            openai_api_base="https://custom.openai.com/v1",
            openai_api_key="sk-custom-key",
            model="gpt-4",
            temperature=0.0,
            max_tokens=100,
            top_p=0.9
        )
        assert result == mock_instance

    def test_describe_with_chat_kwargs(self, openai_credentials):
        """Test describe method returns chat model kwargs."""
        kwargs = {"model": "gpt-4", "temperature": 0.0, "max_tokens": 500}
        dataset = ChatOpenAIDataset(credentials=openai_credentials, kwargs=kwargs)
        description = dataset._describe()
        
        assert description == kwargs

    def test_save_raises_error(self, chat_dataset):
        """Test that save method raises appropriate error."""
        with pytest.raises(DatasetError, match="ChatOpenAIDataset is a read only dataset type"):
            chat_dataset.save(data="test")


class TestCredentialsValidation:
    """Test credential validation across all dataset classes."""

    @pytest.mark.parametrize("dataset_class", [OpenAIEmbeddingsDataset, ChatOpenAIDataset])
    def test_missing_openai_api_base(self, dataset_class):
        """Test that missing openai_api_base raises KeyError."""
        credentials = {"openai_api_key": "sk-test-key"}
        
        with pytest.raises(KeyError, match="openai_api_base"):
            dataset_class(credentials=credentials)

    @pytest.mark.parametrize("dataset_class", [OpenAIEmbeddingsDataset, ChatOpenAIDataset])
    def test_missing_openai_api_key(self, dataset_class):
        """Test that missing openai_api_key raises KeyError."""
        credentials = {"openai_api_base": "https://api.openai.com/v1"}
        
        with pytest.raises(KeyError, match="openai_api_key"):
            dataset_class(credentials=credentials)

    @pytest.mark.parametrize("dataset_class", [OpenAIEmbeddingsDataset, ChatOpenAIDataset])
    def test_empty_credentials_dict(self, dataset_class):
        """Test that empty credentials dict raises KeyError."""
        credentials = {}
        
        with pytest.raises(KeyError):
            dataset_class(credentials=credentials)


class TestIntegration:
    """Integration tests that verify the full workflow without mocking."""

    def test_embeddings_dataset_integration(self, embeddings_dataset):
        """Test OpenAIEmbeddingsDataset integration without API calls."""
        # Verify properties without calling load() to avoid API calls
        assert embeddings_dataset.constructor == OpenAIEmbeddings
        assert embeddings_dataset.openai_api_base == "https://api.openai.com/v1"
        assert embeddings_dataset.openai_api_key == "sk-test-key"
        assert embeddings_dataset.kwargs == {"model": "text-embedding-ada-002"}
        assert embeddings_dataset._describe() == {"model": "text-embedding-ada-002"}

    def test_chat_dataset_integration(self, chat_dataset):
        """Test ChatOpenAIDataset integration without API calls."""
        # Verify properties without calling load() to avoid API calls
        assert chat_dataset.constructor == ChatOpenAI
        assert chat_dataset.openai_api_base == "https://api.openai.com/v1"
        assert chat_dataset.openai_api_key == "sk-test-key"
        assert chat_dataset.kwargs == {"model": "gpt-3.5-turbo", "temperature": 0.7}
        assert chat_dataset._describe() == {"model": "gpt-3.5-turbo", "temperature": 0.7}

    @pytest.mark.parametrize("dataset_class,expected_constructor", [
        (OpenAIEmbeddingsDataset, OpenAIEmbeddings),
        (ChatOpenAIDataset, ChatOpenAI),
    ])
    def test_dataset_type_consistency(self, dataset_class, expected_constructor, openai_credentials):
        """Test that each dataset class returns the correct constructor type."""
        dataset = dataset_class(credentials=openai_credentials)
        assert dataset.constructor == expected_constructor


class TestEdgeCases:
    """Test edge cases and unusual scenarios."""

    def test_special_characters_in_credentials(self):
        """Test dataset works with special characters in credentials."""
        credentials = {
            "openai_api_base": "https://api.openai.com/v1?param=value&other=test",
            "openai_api_key": "sk-test-key-with-dashes_and_underscores"
        }
        
        dataset = OpenAIEmbeddingsDataset(credentials=credentials)
        assert dataset.openai_api_base == credentials["openai_api_base"]
        assert dataset.openai_api_key == credentials["openai_api_key"]

    @patch('kedro_datasets_experimental.langchain._openai.OpenAIEmbeddings')
    def test_load_with_complex_kwargs(self, mock_openai_embeddings, openai_credentials):
        """Test load method with complex nested kwargs."""
        kwargs = {
            "model": "text-embedding-ada-002",
            "openai_api_type": "azure",
            "deployment": "test-deployment",
            "headers": {"Custom-Header": "value"}
        }
        
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance
        
        dataset = OpenAIEmbeddingsDataset(credentials=openai_credentials, kwargs=kwargs)
        result = dataset.load()
        
        mock_openai_embeddings.assert_called_once_with(
            openai_api_base="https://api.openai.com/v1",
            openai_api_key="sk-test-key",
            **kwargs
        )
        assert result == mock_instance

    def test_kwargs_immutability(self, openai_credentials):
        """Test that modifying external kwargs dict doesn't affect dataset."""
        kwargs = {"model": "gpt-3.5-turbo"}
        
        dataset = ChatOpenAIDataset(credentials=openai_credentials, kwargs=kwargs)
        
        # Modify original kwargs
        kwargs["temperature"] = 0.9
        
        # Dataset should still have original kwargs
        assert dataset.kwargs == {"model": "gpt-3.5-turbo"}
        assert dataset._describe() == {"model": "gpt-3.5-turbo"}

