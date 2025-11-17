"""Tests for OpenAI dataset classes."""

from unittest.mock import Mock, patch

import pytest
from kedro.io import DatasetError
from kedro_datasets._utils.abstract_openai_dataset import AbstractOpenAIDataset
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

from kedro_datasets.langchain.chat_openai_dataset import ChatOpenAIDataset
from kedro_datasets.langchain.openai_embeddings_dataset import OpenAIEmbeddingsDataset


@pytest.fixture
def openai_credentials():
    """Fixture for standard OpenAI credentials."""
    return {
        "base_url": "https://api.openai.com/v1",
        "api_key": "sk-test-key",  # pragma: allowlist-secret
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
def test_openai_dataset():
    """Fixture that creates a concrete TestDataset for base class testing."""

    class TestDataset(AbstractOpenAIDataset):
        @property
        def constructor(self):
            return Mock

    return TestDataset


class TestOpenAIDataset:
    """Test the base OpenAIDataset class."""

    def test_init_with_credentials(
        self, test_openai_dataset, openai_credentials, chat_kwargs
    ):
        """Test dataset initialization with credentials."""
        dataset = test_openai_dataset(
            credentials=openai_credentials, kwargs=chat_kwargs
        )

        assert dataset.credentials == openai_credentials
        assert dataset.kwargs == chat_kwargs

    def test_init_without_credentials(self, test_openai_dataset):
        """Test dataset initialization without credentials (uses environment variables)."""
        dataset = test_openai_dataset()

        assert dataset.credentials == {}
        assert dataset.kwargs == {}

    def test_init_with_partial_credentials(self, test_openai_dataset):
        """Test dataset initialization with partial credentials works."""
        credentials = {"openai_api_key": "sk-test-key"}  # pragma: allowlist-secret
        dataset = test_openai_dataset(credentials=credentials)

        assert dataset.credentials == credentials
        assert dataset.kwargs == {}

    def test_describe(self, test_openai_dataset, openai_credentials, chat_kwargs):
        """Test the _describe method returns kwargs."""
        dataset = test_openai_dataset(
            credentials=openai_credentials, kwargs=chat_kwargs
        )
        description = dataset._describe()
        assert description == {
            **{k: "***" for k in openai_credentials.keys()},
            **chat_kwargs,
        }

    def test_save_raises_error(self, test_openai_dataset, openai_credentials):
        """Test that save method raises DatasetError."""
        dataset = test_openai_dataset(credentials=openai_credentials)
        with pytest.raises(
            DatasetError, match="TestDataset is a read only dataset type"
        ):
            dataset.save(data="test")

    def test_load_calls_constructor_with_credentials(
        self, openai_credentials, chat_kwargs
    ):
        """Test that load method calls constructor with correct arguments."""
        mock_constructor = Mock()
        mock_instance = Mock()
        mock_constructor.return_value = mock_instance

        class TestDataset(AbstractOpenAIDataset):
            @property
            def constructor(self):
                return mock_constructor

        dataset = TestDataset(credentials=openai_credentials, kwargs=chat_kwargs)
        result = dataset.load()

        mock_constructor.assert_called_once_with(
            api_key="sk-test-key",  # pragma: allowlist-secret
            base_url="https://api.openai.com/v1",
            model="gpt-3.5-turbo",
            temperature=0.7,
        )
        assert result == mock_instance

    def test_load_without_credentials(self):
        """Test that load method works without credentials (uses environment variables)."""
        mock_constructor = Mock()
        mock_instance = Mock()
        mock_constructor.return_value = mock_instance

        class TestDataset(AbstractOpenAIDataset):
            @property
            def constructor(self):
                return mock_constructor

        dataset = TestDataset()
        result = dataset.load()

        mock_constructor.assert_called_once_with()
        assert result == mock_instance


class TestOpenAIEmbeddingsDataset:
    """Test the OpenAIEmbeddingsDataset class."""

    def test_constructor_property(self, openai_credentials):
        """Test that constructor property returns OpenAIEmbeddings."""
        dataset = OpenAIEmbeddingsDataset(credentials=openai_credentials)
        assert dataset.constructor == OpenAIEmbeddings

    @patch("kedro_datasets.langchain.openai_embeddings_dataset.OpenAIEmbeddings")
    def test_load_with_credentials(
        self, mock_openai_embeddings, openai_credentials, embeddings_kwargs
    ):
        """Test that load method creates OpenAIEmbeddings instance with credentials."""
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance

        dataset = OpenAIEmbeddingsDataset(
            credentials=openai_credentials, kwargs=embeddings_kwargs
        )
        result = dataset.load()

        mock_openai_embeddings.assert_called_once_with(
            api_key="sk-test-key",  # pragma: allowlist-secret
            base_url="https://api.openai.com/v1",
            model="text-embedding-ada-002",
        )
        assert result == mock_instance

    @patch("kedro_datasets.langchain.openai_embeddings_dataset.OpenAIEmbeddings")
    def test_load_without_credentials(self, mock_openai_embeddings):
        """Test that load method works without credentials (uses environment variables)."""
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance

        dataset = OpenAIEmbeddingsDataset()
        result = dataset.load()

        mock_openai_embeddings.assert_called_once_with()
        assert result == mock_instance

    @patch("kedro_datasets.langchain.openai_embeddings_dataset.OpenAIEmbeddings")
    def test_load_with_partial_credentials_api_key_only(self, mock_openai_embeddings):
        """Test that providing only api_key works (base_url falls back to env)."""
        credentials = {"api_key": "sk-test-key"}  # pragma: allowlist-secret
        mock_instance = Mock()
        mock_openai_embeddings.return_value = mock_instance

        dataset = OpenAIEmbeddingsDataset(credentials=credentials)
        result = dataset.load()

        mock_openai_embeddings.assert_called_once_with(
            api_key="sk-test-key"  # pragma: allowlist-secret
        )
        assert result == mock_instance

    def test_describe(self, openai_credentials, embeddings_kwargs):
        """Test the _describe method returns kwargs."""
        dataset = OpenAIEmbeddingsDataset(
            credentials=openai_credentials, kwargs=embeddings_kwargs
        )
        description = dataset._describe()
        assert description == {
            **{k: "***" for k in openai_credentials.keys()},
            **embeddings_kwargs,
        }

    def test_save_raises_error(self, openai_credentials):
        """Test that save method raises DatasetError."""
        dataset = OpenAIEmbeddingsDataset(credentials=openai_credentials)
        with pytest.raises(
            DatasetError, match="OpenAIEmbeddingsDataset is a read only dataset type"
        ):
            dataset.save(data="test")


class TestChatOpenAIDataset:
    """Test the ChatOpenAIDataset class."""

    def test_constructor_property(self, openai_credentials):
        """Test that constructor property returns ChatOpenAI."""
        dataset = ChatOpenAIDataset(credentials=openai_credentials)
        assert dataset.constructor == ChatOpenAI

    @patch("kedro_datasets.langchain.chat_openai_dataset.ChatOpenAI")
    def test_load_with_credentials(
        self, mock_chat_openai, openai_credentials, chat_kwargs
    ):
        """Test that load method creates ChatOpenAI instance with credentials."""
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance

        dataset = ChatOpenAIDataset(credentials=openai_credentials, kwargs=chat_kwargs)
        result = dataset.load()

        mock_chat_openai.assert_called_once_with(
            api_key="sk-test-key",  # pragma: allowlist-secret
            base_url="https://api.openai.com/v1",
            model="gpt-3.5-turbo",
            temperature=0.7,
        )
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_openai_dataset.ChatOpenAI")
    def test_load_without_credentials(self, mock_chat_openai):
        """Test that load method works without credentials (uses environment variables)."""
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance

        dataset = ChatOpenAIDataset()
        result = dataset.load()

        mock_chat_openai.assert_called_once_with()
        assert result == mock_instance

    @patch("kedro_datasets.langchain.chat_openai_dataset.ChatOpenAI")
    def test_load_with_complex_kwargs(self, mock_chat_openai, openai_credentials):
        """Test load method with complex kwargs."""
        kwargs = {
            "model": "gpt-4",
            "temperature": 0.0,
            "max_tokens": 100,
            "top_p": 0.9,
            "frequency_penalty": 0.1,
        }
        mock_instance = Mock()
        mock_chat_openai.return_value = mock_instance

        dataset = ChatOpenAIDataset(credentials=openai_credentials, kwargs=kwargs)
        result = dataset.load()

        mock_chat_openai.assert_called_once_with(
            api_key="sk-test-key",  # pragma: allowlist-secret
            base_url="https://api.openai.com/v1",
            **kwargs
        )
        assert result == mock_instance
