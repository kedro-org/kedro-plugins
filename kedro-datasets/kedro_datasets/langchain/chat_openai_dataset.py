"""Defines an interface to common OpenAI models."""
from langchain_openai import ChatOpenAI
from kedro_datasets._utils.abstract_openai_dataset import AbstractOpenAIDataset

class ChatOpenAIDataset(AbstractOpenAIDataset[ChatOpenAI]):
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
    from kedro_datasets_experimental.langchain import ChatOpenAIDataset

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

    @property
    def constructor(self) -> type[ChatOpenAI]:
        return ChatOpenAI
