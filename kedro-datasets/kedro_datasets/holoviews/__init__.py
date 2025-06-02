"""``AbstractDataset`` implementation to save Holoviews objects as image files."""

from typing import Any

import lazy_loader as lazy

try:
    from .holoviews_writer import HoloviewsWriter
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    HoloviewsWriter: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"holoviews_writer": ["HoloviewsWriter"]}
)
