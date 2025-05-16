"""``AbstractDataset`` implementation to load/save image data."""

from typing import Any

import lazy_loader as lazy

try:
    from .image_dataset import ImageDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    ImageDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"image_dataset": ["ImageDataset"]}
)
