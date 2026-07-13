"""Provides interface to langchain model API objects."""
from typing import Any

import lazy_loader as lazy

try:
    from .chat_anthropic_dataset import ChatAnthropicDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    ChatAnthropicDataset: Any

try:
    from .chat_cohere_dataset import ChatCohereDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    ChatCohereDataset: Any

try:
    from .chat_openai_dataset import ChatOpenAIDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    ChatOpenAIDataset: Any

try:
    from .openai_embeddings_dataset import OpenAIEmbeddingsDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    OpenAIEmbeddingsDataset: Any

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
        "chat_openai_dataset": ["ChatOpenAIDataset"],
        "openai_embeddings_dataset": ["OpenAIEmbeddingsDataset"],
        "chat_anthropic_dataset": ["ChatAnthropicDataset"],
        "chat_cohere_dataset": ["ChatCohereDataset"],
    },
)
