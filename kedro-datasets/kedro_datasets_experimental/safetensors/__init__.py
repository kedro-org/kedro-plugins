"""``AbstractDataset`` implementation to load/save tensors using the SafeTensors library."""

from typing import Any

import lazy_loader as lazy

SafetensorsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"safetensors_dataset": ["SafetensorsDataset"]}
)
