"""``AbstractDataset`` implementation to save matplotlib objects as image files."""

from typing import Any

import lazy_loader as lazy

try:
    from .matplotlib_dataset import MatplotlibDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    MatplotlibDataset: Any

try:
    from .matplotlib_writer import MatplotlibWriter
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    MatplotlibWriter: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "matplotlib_writer": ["MatplotlibWriter"],
        "matplotlib_dataset": ["MatplotlibDataset"],
    },
)
