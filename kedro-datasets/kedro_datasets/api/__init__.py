"""``APIDataSet`` loads the data from HTTP(S) APIs
and returns them into either as string or json Dict.
It uses the python requests library: https://requests.readthedocs.io/en/latest/
"""

import lazy_loader as lazy

__getattr__, __dir__, __all__ = lazy.attach(
    __name__, submod_attrs={"api_dataset": ["APIDataSet"]}
)
