"""``AbstractDataset`` implementation to load/save data from/to a YAML file."""

from typing import Any

import lazy_loader as lazy

try:
    from .yaml_dataset import YAMLDataset
except (ImportError, RuntimeError):
    YAMLDataset: Any

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"yaml_dataset": ["YAMLDataset"]}
)
