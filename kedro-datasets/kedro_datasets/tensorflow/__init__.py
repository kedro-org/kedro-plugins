"""Provides I/O for TensorFlow models."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
TensorFlowModelDataSet: type[TensorFlowModelDataset]
TensorFlowModelDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "tensorflow_model_dataset": ["TensorFlowModelDataSet", "TensorFlowModelDataset"]
    },
)
