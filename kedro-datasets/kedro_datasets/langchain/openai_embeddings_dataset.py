from langchain_openai import OpenAIEmbeddings

from kedro_datasets._utils.abstract_openai_dataset import AbstractOpenAIDataset


class OpenAIEmbeddingsDataset(AbstractOpenAIDataset[OpenAIEmbeddings]):
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
    from kedro_datasets_experimental.langchain import OpenAIEmbeddingsDataset

    # With explicit credentials
    embeddings = OpenAIEmbeddingsDataset(
        credentials={
            "base_url": "<openai-api-base>",
            "api_key": "<openai-api-key>",
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
