"""
Cohere dataset definition.
"""

from typing import Any, NoReturn

from cohere import AsyncClient, Client
from kedro.io import AbstractDataset, DatasetError
from langchain_cohere.llms import Cohere

class CohereDataset(AbstractDataset[None, Cohere]):
    """``CohereDataset`` loads a Cohere `langchain <https://python.langchain.com/>`_ model.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    catalog.yml:

    .. code-block:: yaml
       command:
         type: langchain.CohereDataset
         kwargs:
           model: "command"
           temperature: 0.0
         credentials: cohere


    credentials.yml:

    .. code-block:: yaml
       cohere:
         cohere_api_url: <cohere-api-base>
         cohere_api_key: <cohere-api-key>

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: python
        >>> from kedro_datasets_experimental.langchain import CohereDataset
        >>> llm = CohereDataset(
        ...     credentials={
        ...         "cohere_api_key": "xxx",
        ...     },
        ...     kwargs={
        ...         "model": "command",
        ...         "temperature": 0,
        ...     }
        ... ).load()
        >>>
        >>> # See: https://python.langchain.com/docs/integrations/llms/cohere
        >>> llm.invoke("Hello world!")
    """

    def __init__(self, credentials: dict[str, str], kwargs: dict[str, Any] = None):
        """Constructor.

        Args:
            credentials: must contain `cohere_api_url` and `cohere_api_key`.
            kwargs: keyword arguments passed to the underlying constructor.
        """
        self.cohere_api_url = credentials["cohere_api_url"]
        self.cohere_api_key = credentials["cohere_api_key"]
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        return {**self.kwargs}

    def _save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only data set type")

    def _load(self) -> Cohere:
        llm = Cohere(cohere_api_key="_", **self.kwargs)

        client_kwargs = {
            "api_key": self.cohere_api_key,
        }
        llm.client = Client(**client_kwargs, client_name=llm.user_agent)
        llm.async_client = AsyncClient(**client_kwargs, client_name=llm.user_agent)

        return llm
