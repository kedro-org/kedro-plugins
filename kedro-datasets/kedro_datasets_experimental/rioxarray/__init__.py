"""``AbstractDataset`` implementation to load/save data from/to a geospatial raster files."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
GeoTIFFDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"geotiff_dataset": ["GeoTIFFDataset"]}
)
