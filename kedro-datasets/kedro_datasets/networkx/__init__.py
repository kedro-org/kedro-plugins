"""``AbstractDataset`` implementation to save and load graphs in JSON,
GraphML and GML formats using NetworkX."""

from typing import Any

import lazy_loader as lazy

try:
    from .gml_dataset import GMLDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GMLDataset: Any

try:
    from .graphml_dataset import GraphMLDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    GraphMLDataset: Any

try:
    from .json_dataset import JSONDataset
except (ImportError, RuntimeError):
    # For documentation builds that might fail due to dependency issues
    # https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
    JSONDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "gml_dataset": ["GMLDataset"],
        "graphml_dataset": ["GraphMLDataset"],
        "json_dataset": ["JSONDataset"],
    },
)
