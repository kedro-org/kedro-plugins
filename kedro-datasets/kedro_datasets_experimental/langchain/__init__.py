"""Provides interface to langchain model API objects."""
from typing import Any

import lazy_loader as lazy

try:
    from .langchain_prompt_dataset import LangChainPromptDataset

except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    LangChainPromptDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "langchain_prompt_dataset": ["LangChainPromptDataset"],
    },
)
