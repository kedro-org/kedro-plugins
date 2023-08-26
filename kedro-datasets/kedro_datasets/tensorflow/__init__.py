"""Provides I/O for TensorFlow Models."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
TensorFlowModelDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"tensorflow_model_dataset": ["TensorFlowModelDataSet"]}
)
