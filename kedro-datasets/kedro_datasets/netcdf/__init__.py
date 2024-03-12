"""``NetCDFDataset`` is an ``AbstractDataset`` to save and load NetCDF files."""
from __future__ import annotations

from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
NetCDFDataset: type[NetCDFDataset]
NetCDFDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"netcdf_dataset": ["NetCDFDataset"]}
)
