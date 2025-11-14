"""Defines an interface to common Anthropic models."""

from typing import Any, NoReturn

from kedro.io import AbstractDataset, DatasetError
from langchain_anthropic import ChatAnthropic


class ChatAnthropicDataset(AbstractDataset[None, ChatAnthropic]):
    """
    `ChatAnthropicDataset` loads a ChatAnthropic [langchain](https://python.langchain.com/) model.

    ### Example usage for the [YAML API](https://docs.kedro.org/en/stable/catalog-data/data_catalog_yaml_examples/)

    **catalog.yml**

    ```yaml
    claude_instant_1:
        type: langchain.ChatAnthropicDataset
        kwargs:
            model: "claude-instant-1"
            temperature: 0.0
        credentials: anthropic  # Optional, can use environment variables instead
    ```

    **credentials.yml** (optional if using environment variables)
    If credentials are passed through `credentials.yml`, they take precedence over environment variables.

    ```yaml
    anthropic:
        base_url: <anthropic-api-base>  # Optional, defaults to Anthropic default
        api_key: <anthropic-api-key>   # Optional if ANTHROPIC_API_KEY is set
    ```

    **Or use environment variables:**
    ```bash
    export ANTHROPIC_API_KEY=<your-api-key>
    export ANTHROPIC_API_URL=<anthropic-api-base>  # Optional
    ```

    ### Example usage for the [Python API](https://docs.kedro.org/en/stable/catalog-data/advanced_data_catalog_usage/)

    ```python
    from kedro_datasets.langchain import ChatAnthropicDataset

    # With explicit credentials
    llm = ChatAnthropicDataset(
        credentials={
            "base_url": "xxx",
            "api_key": "xxx",  # pragma: allowlist secret
        },
        kwargs={
            "model": "claude-instant-1",
            "temperature": 0.0,
        },
    ).load()

    # Or without credentials (using environment variables)
    llm = ChatAnthropicDataset(
        kwargs={
            "model": "claude-instant-1",
            "temperature": 0.0,
        },
    ).load()

    # See: https://python.langchain.com/docs/integrations/chat/anthropic
    llm.invoke("Hello world!")
    ```

    """

    def __init__(self, credentials: dict[str, str] = {}, kwargs: dict[str, Any] = {}):
        """Constructor.

        Args:
            credentials (Optional): contains `api_key` and `base_url`.
                If not provided, will use environment variables ANTHROPIC_API_KEY and ANTHROPIC_API_URL.
            kwargs: keyword arguments passed to the ChatAnthropic constructor.
        """
        self.credentials = credentials or {}
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        """Returns a description of the dataset.

        Returns:
            dict[str, Any]: Dictionary containing the kwargs passed to ChatAnthropic.
        """
        credentials = (
            {k: "***" for k in self.credentials.keys()} if self.credentials else {}
        )
        return {**credentials, **self.kwargs}

    def save(self, data: None) -> NoReturn:
        """Save operation is not supported for ChatAnthropicDataset.

        Raises:
            DatasetError: Always raised as this dataset is read-only.
        """
        raise DatasetError(f"{self.__class__.__name__} is a read only dataset type")

    def load(self) -> ChatAnthropic:
        """Load and return a ChatAnthropic model instance.

        Constructs a ChatAnthropic instance using the provided kwargs and optional
        credentials. If credentials are not provided, the ChatAnthropic instance
        will automatically use environment variables ANTHROPIC_API_KEY and
        ANTHROPIC_API_URL for authentication.

        Returns:
            ChatAnthropic: A configured ChatAnthropic model instance.
        """
        return ChatAnthropic(**self.credentials, **self.kwargs)  # type: ignore[arg-type]
