from typing import Any, NoReturn

from kedro.io import AbstractDataset, DatasetError
from langchain_openai import OpenAIEmbeddings


class OpenAIEmbeddingsDataset(AbstractDataset[None, OpenAIEmbeddings]):
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
    from kedro_datasets.langchain import OpenAIEmbeddingsDataset

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

    def load(self) -> OpenAIEmbeddings:
        """Load and return an OpenAI model instance.

        Constructs an OpenAI instance using the provided kwargs and optional
        credentials. If credentials are not provided, the OpenAI instance
        will automatically use environment variables OPENAI_API_KEY and
        OPENAI_API_BASE for authentication.

        Returns:
            OPENAI_TYPE: A configured OpenAI model instance.
        """
        return OpenAIEmbeddings(**self.credentials, **self.kwargs)  # type: ignore[arg-type]
