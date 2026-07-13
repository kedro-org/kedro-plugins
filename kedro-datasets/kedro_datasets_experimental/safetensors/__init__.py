"""``AbstractDataset`` implementation to load/save tensors using the SafeTensors library."""

from typing import Any

import lazy_loader as lazy

try:
    from .safetensors_dataset import SafetensorsDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    SafetensorsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"safetensors_dataset": ["SafetensorsDataset"]}
)
