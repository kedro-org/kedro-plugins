import matplotlib
import pytest

# Use a non-GUI backend for all doctests (important for CI and Windows)
matplotlib.use("Agg")  # Must be set before any pyplot import


@pytest.fixture(autouse=True)
def add_tmp_path(doctest_namespace, tmp_path):
    doctest_namespace["tmp_path"] = tmp_path
