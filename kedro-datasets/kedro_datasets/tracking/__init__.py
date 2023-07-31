"""Dataset implementations to save data for Kedro Experiment Tracking"""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
JSONDataSet: Any
MetricsDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "json_dataset": ["JSONDataSet"],
        "metrics_dataset": ["MetricsDataSet"],
    },
)
