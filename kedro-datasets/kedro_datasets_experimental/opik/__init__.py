"""``AbstractDataset`` implementations for Opik integration."""

from typing import Any

import lazy_loader as lazy

try:
    from .evaluation_dataset import EvaluationDataset
    from .prompt_dataset import PromptDataset
    from .trace_dataset import TraceDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    EvaluationDataset: Any
    PromptDataset: Any
    TraceDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "evaluation_dataset": ["EvaluationDataset"],
        "prompt_dataset": ["PromptDataset"],
        "trace_dataset": ["TraceDataset"],
    },
)
