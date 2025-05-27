"""Provides I/O for TensorFlow models."""

from typing import Any

import lazy_loader as lazy

try:
    from .tensorflow_model_dataset import TensorFlowModelDataset
except (ImportError, RuntimeError):
    TensorFlowModelDataset: Any

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"tensorflow_model_dataset": ["TensorFlowModelDataset"]},
)
