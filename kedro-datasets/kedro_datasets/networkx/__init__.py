"""``AbstractDataset`` implementation to save and load graphs in JSON,
GraphML and GML formats using NetworkX."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GMLDataSet: type[GMLDataset]
GMLDataset: Any
GraphMLDataSet: type[GraphMLDataset]
GraphMLDataset: Any
JSONDataSet: type[JSONDataset]
JSONDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={
        "gml_dataset": ["GMLDataSet", "GMLDataset"],
        "graphml_dataset": ["GraphMLDataSet", "GraphMLDataset"],
        "json_dataset": ["JSONDataSet", "JSONDataset"],
    },
)
