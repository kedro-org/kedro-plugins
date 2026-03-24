import pandas as pd
import pytest
from packaging.version import parse

pytestmark = pytest.mark.skipif(
    parse(pd.__version__) >= parse("3"),
    reason="Dask is not compatible with pandas 3.x yet",
)
