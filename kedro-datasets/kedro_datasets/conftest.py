import pytest


@pytest.fixture(autouse=True)
def add_tmp_path(doctest_namespace, tmp_path):
    doctest_namespace["tmp_path"] = tmp_path
