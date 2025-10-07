"""``AbstractDataset`` implementations for Langfuse integration."""

from typing import Any

import lazy_loader as lazy

try:
    from .langfuse_trace_dataset import LangfuseTraceDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    LangfuseTraceDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"langfuse_trace_dataset": ["LangfuseTraceDataset"]}
)