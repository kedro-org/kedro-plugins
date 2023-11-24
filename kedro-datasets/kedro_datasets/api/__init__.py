"""``APIDataset`` loads the data from HTTP(S) APIs
and returns them into either as string or json Dict.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
APIDataset: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"api_dataset": ["APIDataset"]}
)
