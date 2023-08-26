"""``AbstractDataSet`` implementations that produce pandas DataFrames."""
from typing import Any

import lazy_loader as lazy

# https://github.com/pylint-dev/pylint/issues/4300#issuecomment-1043601901
CSVDataSet: Any
GenericDataSet: Any

__getattr__, __dir__, __all__ = lazy.attach(
    __name__,
    submod_attrs={"csv_dataset": ["CSVDataSet"], "generic_dataset": ["GenericDataSet"]},
)
