"""``AbstractDataSet`` implementation to load/save data from/to a redis db."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
PickleDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"redis_dataset": ["PickleDataSet"]}
)
