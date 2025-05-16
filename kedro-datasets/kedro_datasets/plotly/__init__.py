"""``AbstractDataset`` implementations to load/save a plotly figure from/to a JSON
file."""

from typing import Any

import lazy_loader as lazy

try:
    from .html_dataset import HTMLDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    HTMLDataset: Any

try:
    from .json_dataset import JSONDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    JSONDataset: Any

try:
    from .plotly_dataset import PlotlyDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    PlotlyDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "html_dataset": ["HTMLDataset"],
        "json_dataset": ["JSONDataset"],
        "plotly_dataset": ["PlotlyDataset"],
    },
)
