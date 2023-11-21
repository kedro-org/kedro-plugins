"""``AbstractDataset`` implementation to save and load graphs in JSON,
GraphML and GML formats using NetworkX."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GMLDataset: Any
GraphMLDataset: Any
JSONDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "gml_dataset": ["GMLDataset"],
        "graphml_dataset": ["GraphMLDataset"],
        "json_dataset": ["JSONDataset"],
    },
)
