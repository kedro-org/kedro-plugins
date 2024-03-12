"""``AbstractDataset`` implementation to save matplotlib objects as image files."""
from typing import Any

import lazy_loader as lazy

MatplotlibWriter: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"matplotlib_writer": ["MatplotlibWriter"]}
)
