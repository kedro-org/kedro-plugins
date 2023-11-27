"""``GeoJSONDataset`` is an ``AbstractVersionedDataset`` to save and load GeoJSON files."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GeoJSONDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"geojson_dataset": ["GeoJSONDataset"]}
)
