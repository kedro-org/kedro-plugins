"""``AbstractDataset`` implementations for Opik integration."""

from typing import Any

import lazy_loader as lazy

try:
    from .opik_prompt_dataset import OpikPromptDataset
    from .opik_trace_dataset import OpikTraceDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    OpikPromptDataset: Any
    OpikTraceDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "opik_prompt_dataset": ["OpikPromptDataset"],
        "opik_trace_dataset": ["OpikTraceDataset"],
    },
)
