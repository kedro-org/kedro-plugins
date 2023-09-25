"""Dataset implementations to save data for Kedro Experiment Tracking."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
JSONDataSet: type[JSONDataset]
JSONDataset: Any
MetricsDataSet: type[MetricsDataset]
MetricsDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "json_dataset": ["JSONDataSet", "JSONDataset"],
        "metrics_dataset": ["MetricsDataSet", "MetricsDataset"],
    },
)
