"""Provides interface to langchain model API objects."""
from typing import Any

import lazy_loader as lazy

try:
    from ._anthropic import ChatAnthropicDataset
    from ._cohere import ChatCohereDataset
    from ._openai import ChatOpenAIDataset, OpenAIEmbeddingsDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ChatAnthropicDataset: Any
    ChatOpenAIDataset: Any
    OpenAIEmbeddingsDataset: Any
    ChatCohereDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "_openai": ["ChatOpenAIDataset", "OpenAIEmbeddingsDataset"],
        "_anthropic": ["ChatAnthropicDataset"],
        "_cohere": ["ChatCohereDataset"],
    },
)
