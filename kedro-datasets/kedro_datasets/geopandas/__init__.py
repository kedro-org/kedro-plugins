"""``GeoJSONDataSet`` is an ``AbstractVersionedDataSet`` to save and load GeoJSON files."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GeoJSONDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"geojson_dataset": ["GeoJSONDataSet"]}
)
