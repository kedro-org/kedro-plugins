"""Defines a general interface to langchain model APIs."""


from abc import abstractmethod
from typing import Any, Dict, NoReturn, TypeVar, Generic

from kedro.io import AbstractDataset, DatasetError
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import OpenAIEmbeddings


OPENAI_TYPE = TypeVar("OPENAI_TYPE")


class OpenAIDataset(AbstractDataset[None, OPENAI_TYPE], Generic[OPENAI_TYPE]):
    """OpenAI dataset used to access credentials at runtime.
    """
    @property
    @abstractmethod
    def constructor(self) -> OPENAI_TYPE:
        """Return the OpenAI class to construct in the _load method."""

    def __init__(self, credentials: Dict[str, str], kwargs: Dict[str, Any] = None):
        """Constructor.

        Args:
            credentials: must contain `openai_api_base` and `openai_api_key`.
            kwargs: keyword arguments passed to the underlying constructor.
        """
        self.openai_api_base = credentials["openai_api_base"]
        self.openai_api_key = credentials["openai_api_key"]
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        return {**self.kwargs}

    def _save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only data set type")

    def _load(self) -> OPENAI_TYPE:
        return self.constructor(
            openai_api_base=self.openai_api_base,
            openai_api_key=self.openai_api_key,
            **self.kwargs,
        )


class OpenAIEmbeddingsDataset(OpenAIDataset[OpenAIEmbeddings]):
    """``OpenAIEmbeddingsDataset`` loads a OpenAIEmbeddings `langchain <https://python.langchain.com/>`_ model.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    catalog.yml:

    .. code-block:: yaml
       gpt_3_5_turbo:
         type: langchain.openai.ChatOpenAIDataSet
         kwargs:
           model: "gpt-3.5-turbo"
           temperature: 0.0
         credentials: openai


    credentials.yml:

    .. code-block:: yaml
       openai:
         openai_api_base: <openai-api-base>
         openai_api_key: <openai-api-key>
    """

    @property
    def constructor(self) -> type[OpenAIEmbeddings]:
        return OpenAIEmbeddings


class ChatOpenAIDataset(OpenAIDataset[ChatOpenAI]):
    """``ChatOpenAIDataset`` loads a ChatOpenAI `langchain <https://python.langchain.com/>`_ model.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    catalog.yml:

    .. code-block:: yaml
       gpt_3_5_turbo:
         type: langchain.openai.ChatOpenAIDataSet
         kwargs:
           model: "gpt-3.5-turbo"
           temperature: 0.0
         credentials: openai


    credentials.yml:

    .. code-block:: yaml
       openai:
         openai_api_base: <openai-api-base>
         openai_api_key: <openai-api-key>
    """
    @property
    def constructor(self) -> type[ChatOpenAI]:
        return ChatOpenAI
