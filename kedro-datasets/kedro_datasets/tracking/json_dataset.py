"""``JSONDataSet`` saves data to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
The ``JSONDataSet`` is part of Kedro Experiment Tracking. The dataset is versioned by default.
"""
from typing import NoReturn

from kedro.io.core import DataSetError

from kedro_datasets.json import json_dataset


class JSONDataSet(json_dataset.JSONDataSet):
    """``JSONDataSet`` saves data to a JSON file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
    The ``JSONDataSet`` is part of Kedro Experiment Tracking.
    The dataset is write-only and it is versioned by default.

    Example usage for the
    `YAML API <https://docs.kedro.org/en/stable/data/\
    data_catalog_yaml_examples.html#data-catalog-yaml-examples>`_:

    .. code-block:: yaml

        cars:
          type: tracking.JSONDataSet
          filepath: data/09_tracking/cars.json

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog.html#use-the-data-catalog-with-the-code-api>`_:
    ::

        >>> from kedro_datasets.tracking import JSONDataSet
        >>>
        >>> data = {'col1': 1, 'col2': 0.23, 'col3': 0.002}
        >>>
        >>> data_set = JSONDataSet(filepath="test.json")
        >>> data_set.save(data)

    """

    versioned = True

    def _load(self) -> NoReturn:
        raise DataSetError(f"Loading not supported for '{self.__class__.__name__}'")
