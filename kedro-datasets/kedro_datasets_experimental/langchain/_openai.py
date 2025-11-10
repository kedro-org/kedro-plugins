"""Defines an interface to common OpenAI models."""

from abc import abstractmethod
from typing import Any, Generic, NoReturn, TypeVar

from kedro.io import AbstractDataset, DatasetError
from langchain_openai import ChatOpenAI, OpenAIEmbeddings

OPENAI_TYPE = TypeVar("OPENAI_TYPE")


class OpenAIDataset(AbstractDataset[None, OPENAI_TYPE], Generic[OPENAI_TYPE]):
    """OpenAI dataset used to access credentials at runtime."""

    @property
    @abstractmethod
    def constructor(self) -> OPENAI_TYPE:
        """Return the OpenAI class to construct in the _load method."""

    def __init__(self, credentials: dict[str, str] = None, kwargs: dict[str, Any] = None):
        """Constructor.

        Args:
            credentials (Optional): contains `openai_api_key` and `openai_api_base`.
                If not provided, will use environment variables OPENAI_API_KEY and OPENAI_API_BASE.
            kwargs: keyword arguments passed to the underlying constructor.
        """
        self.credentials = credentials or {}
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        """Returns a description of the dataset.

        Returns:
            dict[str, Any]: Dictionary containing the kwargs passed to the OpenAI constructor.
        """
        return {**self.kwargs}

    def save(self, data: None) -> NoReturn:
        """Save operation is not supported for OpenAI datasets.

        Raises:
            DatasetError: Always raised as this dataset is read-only.
        """
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def load(self) -> OPENAI_TYPE:
        """Load and return an OpenAI model instance.

        Constructs an OpenAI instance using the provided kwargs and optional
        credentials. If credentials are not provided, the OpenAI instance
        will automatically use environment variables OPENAI_API_KEY and
        OPENAI_API_BASE for authentication.

        Returns:
            OPENAI_TYPE: A configured OpenAI model instance.
        """
        init_kwargs = {**self.kwargs}
        if "openai_api_key" in self.credentials:
            init_kwargs["api_key"] = self.credentials["openai_api_key"]
        if "openai_api_base" in self.credentials:
            init_kwargs["base_url"] = self.credentials["openai_api_base"]

        return self.constructor(**init_kwargs)


class OpenAIEmbeddingsDataset(OpenAIDataset[OpenAIEmbeddings]):
    """
    `OpenAIEmbeddingsDataset` loads an OpenAIEmbeddings [langchain](https://python.langchain.com/) model.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)

    **catalog.yml**

    ```yaml
    text_embedding_ada_002:
        type: langchain.OpenAIEmbeddingsDataset
        kwargs:
            model: "text-embedding-ada-002"
        credentials: openai  # Optional, can use environment variables instead
    ```

    **credentials.yml** (optional if using environment variables)

    ```yaml
    openai:
        openai_api_base: <openai-api-base>  # Optional, defaults to OpenAI default
        openai_api_key: <openai-api-key>   # Optional if OPENAI_API_KEY is set
    ```

    **Or use environment variables:**
    ```bash
    export OPENAI_API_KEY=<your-api-key>
    export OPENAI_API_BASE=<openai-api-base>  # Optional
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

    ```python
    from kedro_datasets_experimental.langchain import OpenAIEmbeddingsDataset

    # With explicit credentials
    embeddings = OpenAIEmbeddingsDataset(
        credentials={
            "openai_api_base": "<openai-api-base>",
            "openai_api_key": "<openai-api-key>",
        },
        kwargs={
            "model": "text-embedding-ada-002",
        },
    ).load()

    # Or without credentials (using environment variables)
    embeddings = OpenAIEmbeddingsDataset(
        kwargs={
            "model": "text-embedding-ada-002",
        },
    ).load()

    # See: https://python.langchain.com/docs/integrations/text_embedding/openai
    embeddings.embed_query("Hello world!")
    ```

    """

    @property
    def constructor(self) -> type[OpenAIEmbeddings]:
        return OpenAIEmbeddings


class ChatOpenAIDataset(OpenAIDataset[ChatOpenAI]):
    """
    `ChatOpenAIDataset` loads a ChatOpenAI [langchain](https://python.langchain.com/) model.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)

    **catalog.yml**

    ```yaml
    gpt_3_5_turbo:
        type: langchain.ChatOpenAIDataset
        kwargs:
            model: "gpt-3.5-turbo"
            temperature: 0.0
        credentials: openai  # Optional, can use environment variables instead
    ```

    **credentials.yml** (optional if using environment variables)

    ```yaml
    openai:
        openai_api_base: <openai-api-base>  # Optional, defaults to OpenAI default
        openai_api_key: <openai-api-key>   # Optional if OPENAI_API_KEY is set
    ```

    **Or use environment variables:**
    ```bash
    export OPENAI_API_KEY=<your-api-key>
    export OPENAI_API_BASE=<openai-api-base>  # Optional
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

    ```python
    from kedro_datasets_experimental.langchain import ChatOpenAIDataset

    # With explicit credentials
    llm = ChatOpenAIDataset(
        credentials={
            "openai_api_base": "<openai-api-base>",
            "openai_api_key": "<openai-api-key>",
        },
        kwargs={
            "model": "gpt-3.5-turbo",
            "temperature": 0.0,
        },
    ).load()

    # Or without credentials (using environment variables)
    llm = ChatOpenAIDataset(
        kwargs={
            "model": "gpt-3.5-turbo",
            "temperature": 0.0,
        },
    ).load()

    # See: https://python.langchain.com/docs/integrations/chat/openai
    llm.invoke("Hello world!")
    ```

    """

    @property
    def constructor(self) -> type[ChatOpenAI]:
        return ChatOpenAI
