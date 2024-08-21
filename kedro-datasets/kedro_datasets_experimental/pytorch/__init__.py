"""``AbstractDataset`` implementation to load/save torch models using PyTorch's built-in methods """

from typing import Any

import lazy_loader as lazy

PyTorchDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"pytorch_dataset": ["PyTorchDataset"]}
)
