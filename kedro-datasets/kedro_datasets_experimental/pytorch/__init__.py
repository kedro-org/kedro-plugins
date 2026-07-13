"""``AbstractDataset`` implementation to load/save torch models using PyTorch's built-in methods """

from typing import Any

import lazy_loader as lazy

try:
    from .pytorch_dataset import PyTorchDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PyTorchDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"pytorch_dataset": ["PyTorchDataset"]}
)
