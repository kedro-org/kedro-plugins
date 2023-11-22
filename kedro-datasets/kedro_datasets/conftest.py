import pytest


@pytest.fixture(autouse=True)
<<<<<<< HEAD
def add_tmp_path(doctest_namespace, tmp_path):
=======
def add_np(doctest_namespace, tmp_path):
>>>>>>> 16c6d5e (test(datasets): fix `dask.ParquetDataset` doctests (#439))
    doctest_namespace["tmp_path"] = tmp_path
