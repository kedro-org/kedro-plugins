"""``AbstractDataset`` implementations to load/save a plotly figure from/to a JSON
file."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
JSONDataSet: type[JSONDataset]
JSONDataset: Any
PlotlyDataSet: type[PlotlyDataset]
PlotlyDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "json_dataset": ["JSONDataSet", "JSONDataset"],
        "plotly_dataset": ["PlotlyDataSet", "PlotlyDataset"],
    },
)
