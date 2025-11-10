"""
Cohere dataset definition.
"""

from typing import Any, NoReturn

from kedro.io import AbstractDataset, DatasetError
from langchain_cohere import ChatCohere


class ChatCohereDataset(AbstractDataset[None, ChatCohere]):
    """
    `ChatCohereDataset` loads a ChatCohere [langchain](https://python.langchain.com/) model.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)

    **catalog.yml**

    ```yaml
    command:
        type: langchain.ChatCohereDataset
        kwargs:
            model: "command"
            temperature: 0.0
        credentials: cohere  # Optional, can use environment variables instead
    ```

    **credentials.yml** (optional if using environment variables)

    ```yaml
    cohere:
        cohere_api_url: <cohere-api-base>  # Optional, defaults to https://api.cohere.com
        cohere_api_key: <cohere-api-key>   # Optional if COHERE_API_KEY is set
    ```

    **Or use environment variables:**
    ```bash
    export COHERE_API_KEY=<your-api-key>
    export CO_API_URL=<cohere-api-base>  # Optional
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

    ```python
    from kedro_datasets_experimental.langchain import ChatCohereDataset

    # With explicit credentials
    llm = ChatCohereDataset(
        credentials={
            "cohere_api_key": "xxx",
            "cohere_api_url": "xxx",
        },
        kwargs={
            "model": "command",
            "temperature": 0.0,
        },
    ).load()

    # Or without credentials (using environment variables)
    llm = ChatCohereDataset(
        kwargs={
            "model": "command",
            "temperature": 0.0,
        },
    ).load()

    # See: https://python.langchain.com/docs/integrations/chat/cohere
    llm.invoke("Hello world!")
    ```

    """

    def __init__(self, credentials: dict[str, str] = None, kwargs: dict[str, Any] = None):
        """Constructor.

        Args:
            credentials (Optional): contains `cohere_api_key` and `cohere_api_url`.
                If not provided, will use environment variables COHERE_API_KEY and CO_API_URL.
            kwargs: keyword arguments passed to the ChatCohere constructor.
        """
        self.credentials = credentials or {}
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        """Returns a description of the dataset.

        Returns:
            dict[str, Any]: Dictionary containing the kwargs passed to ChatCohere.
        """
        return {**self.kwargs}

    def save(self, data: None) -> NoReturn:
        """Save operation is not supported for ChatCohereDataset.

        Raises:
            DatasetError: Always raised as this dataset is read-only.
        """
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def load(self) -> ChatCohere:
        """Load and return a ChatCohere model instance.

        Constructs a ChatCohere instance using the provided kwargs and optional
        credentials. If credentials are not provided, the ChatCohere instance
        will automatically use environment variables COHERE_API_KEY and
        CO_API_URL for authentication.

        Returns:
            ChatCohere: A configured ChatCohere model instance.
        """
        init_kwargs = {**self.kwargs}
        if "cohere_api_key" in self.credentials:
            init_kwargs["cohere_api_key"] = self.credentials["cohere_api_key"]
        if "cohere_api_url" in self.credentials:
            init_kwargs["base_url"] = self.credentials["cohere_api_url"]

        return ChatCohere(**init_kwargs)
