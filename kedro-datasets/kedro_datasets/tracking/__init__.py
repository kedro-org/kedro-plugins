"""Dataset implementations to save data for Kedro Experiment Tracking."""

import warnings
from typing import Any

import lazy_loader as lazy

from kedro_datasets import KedroDeprecationWarning

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
JSONDataset: Any
MetricsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "json_dataset": ["JSONDataset"],
        "metrics_dataset": ["MetricsDataset"],
    },
)

warnings.warn(
    "`tracking.JSONDataset` and `tracking.MetricsDataset` are deprecated. These datasets will be removed in kedro-datasets 7.0.0",
    KedroDeprecationWarning,
    stacklevel=2,
)
