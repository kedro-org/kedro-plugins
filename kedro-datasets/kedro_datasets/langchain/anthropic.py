"""Defines an interface to common Anthropic models."""

from typing import Any, Dict, NoReturn

from kedro.io import AbstractDataSet, DatasetError
from langchain.chat_models import ChatAnthropic


class ChatAnthropicDataset(AbstractDataSet[None, ChatAnthropic]):
    """``ChatOpenAIDataset`` loads a ChatAnthropic `langchain <https://python.langchain.com/>`_ model.

    Example usage for the :doc:`YAML API <kedro:data/data_catalog_yaml_examples>`:

    catalog.yml:

    .. code-block:: yaml
       claude_instant_1:
         type: langchain.anthropic.ChatAnthropicDataset
         kwargs:
           model: "claude-instant-1"
           temperature: 0.0
         credentials: anthropic


    credentials.yml:

    .. code-block:: yaml
       anthropic:
         anthropic_api_url: <anthropic-api-base>
         anthropic_api_key: <anthropic-api-key>
    """

    def __init__(self, credentials: Dict[str, str], kwargs: Dict[str, Any] = None):
        """Constructor.

        Args:
            credentials: must contain `anthropic_api_url` and `anthropic_api_key`.
            kwargs: keyword arguments passed to the ChatAnthropic constructor.
        """
        self.anthropic_api_url = credentials["anthropic_api_url"]
        self.anthropic_api_key = credentials["anthropic_api_key"]
        self.kwargs = kwargs or {}

    def _describe(self) -> dict[str, Any]:
        return {**self.kwargs}

    def _save(self, data: None) -> NoReturn:
        raise DatasetError(f"{self.__class__.__name__} is a read only data set type")

    def _load(self) -> ChatAnthropic:
        return ChatAnthropic(
            anthropic_api_url=self.anthropic_api_url,
            anthropic_api_key=self.anthropic_api_key,
            **self.kwargs,
        )
