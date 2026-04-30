import sys
from pathlib import Path

import matplotlib
import pytest

_314_IGNORE = {"tensorflow", "geopandas"}


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    if sys.version_info >= (3, 14) and collection_path.parent.name in _314_IGNORE:
        return True
    return None


# Use a non-GUI backend for all doctests (important for CI and Windows)
matplotlib.use("Agg")  # Must be set before any pyplot import


@pytest.fixture(autouse=True)
def add_tmp_path(doctest_namespace, tmp_path):
    doctest_namespace["tmp_path"] = tmp_path
