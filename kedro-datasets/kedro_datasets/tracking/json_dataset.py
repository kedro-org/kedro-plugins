"""``JSONDataset`` saves data to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
The ``JSONDataset`` is part of Kedro Experiment Tracking. The dataset is versioned by default.
"""
from typing import NoReturn

from kedro.io.core import DatasetError

from kedro_datasets.json import json_dataset


class JSONDataset(json_dataset.JSONDataset):
    """``JSONDataset`` saves data to a JSON file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
    The ``JSONDataset`` is part of Kedro Experiment Tracking.
    The dataset is write-only and it is versioned by default.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: tracking.JSONDataset
          filepath: data/09_tracking/cars.json

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.tracking import JSONDataset
        >>>
        >>> data = {"col1": 1, "col2": 0.23, "col3": 0.002}
        >>>
        >>> dataset = JSONDataset(filepath=tmp_path / "test.json")
        >>> dataset.save(data)

    """

    versioned = True

    def _load(self) -> NoReturn:
        raise DatasetError(f"Loading not supported for '{self.__class__.__name__}'")
