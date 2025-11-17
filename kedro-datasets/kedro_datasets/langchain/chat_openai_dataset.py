"""Defines an interface to common OpenAI models."""
from typing import Any, NoReturn

from kedro.io import AbstractDataset, DatasetError
from langchain_openai import ChatOpenAI


class ChatOpenAIDataset(AbstractDataset[None, ChatOpenAI]):
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
    If credentials are passed through `credentials.yml`, they take precedence over environment variables.

    ```yaml
    openai:
        base_url: <openai-api-base>  # Optional, defaults to OpenAI default
        api_key: <openai-api-key>   # Optional if OPENAI_API_KEY is set
    ```

    **Or use environment variables:**
    ```bash
    export OPENAI_API_KEY=<your-api-key>
    export OPENAI_API_BASE=<openai-api-base>  # Optional
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

    ```python
    from kedro_datasets.langchain import ChatOpenAIDataset

    # With explicit credentials
    llm = ChatOpenAIDataset(
        credentials={
            "base_url": "<openai-api-base>",
            "api_key": "<openai-api-key>",
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

    def __init__(self, credentials: dict[str, str] = {}, kwargs: dict[str, Any] = {}):
        """Constructor.

        Args:
            credentials (Optional): contains `api_key` and `base_url`.
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
        credentials = (
            {k: "***" for k in self.credentials.keys()} if self.credentials else {}
        )
        return {**credentials, **self.kwargs}

    def save(self, data: None) -> NoReturn:
        """Save operation is not supported for OpenAI datasets.

        Raises:
            DatasetError: Always raised as this dataset is read-only.
        """
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def load(self) -> ChatOpenAI:
        """Load and return an OpenAI model instance.

        Constructs an OpenAI instance using the provided kwargs and optional
        credentials. If credentials are not provided, the OpenAI instance
        will automatically use environment variables OPENAI_API_KEY and
        OPENAI_API_BASE for authentication.

        Returns:
            OPENAI_TYPE: A configured OpenAI model instance.
        """
        return ChatOpenAI(**self.credentials, **self.kwargs)  # type: ignore[arg-type]
