"""
This file contains the fixtures that are reusable by any tests within
this directory. You don't need to import the fixtures as pytest will
discover them automatically. More info here:
https://docs.pytest.org/en/latest/fixture.html
"""

import pytest
from datasets import Dataset, DatasetDict, IterableDatasetDict


@pytest.fixture
def hf_dataset():
    return Dataset.from_dict({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})


@pytest.fixture
def dataset_dict():
    return DatasetDict(
        {
            "data": Dataset.from_dict({"col1": [1, 2], "col2": ["a", "b"]}),
            "labels": Dataset.from_dict({"col1": [3], "col2": ["c"]}),
        }
    )


@pytest.fixture
def iterable_dataset():
    return Dataset.from_dict(
        {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
    ).to_iterable_dataset()


@pytest.fixture
def iterable_dataset_dict():
    return IterableDatasetDict(
        {
            "data": Dataset.from_dict(
                {"col1": [1, 2], "col2": ["a", "b"]}
            ).to_iterable_dataset(),
            "labels": Dataset.from_dict(
                {"col1": [3], "col2": ["c"]}
            ).to_iterable_dataset(),
        }
    )
