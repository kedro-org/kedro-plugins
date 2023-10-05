"""``MetricsDataset`` saves data to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
The ``MetricsDataset`` is part of Kedro Experiment Tracking. The dataset is versioned by default
and only takes metrics of numeric values.
"""
import json
import warnings
from typing import Dict, NoReturn

from kedro.io.core import DatasetError, get_filepath_str

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets.json import json_dataset


class MetricsDataset(json_dataset.JSONDataset):
    """``MetricsDataset`` saves data to a JSON file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file. The
    ``MetricsDataset`` is part of Kedro Experiment Tracking. The dataset is write-only,
    it is versioned by default and only takes metrics of numeric values.

    Example usage for the
    `YAML API <https://kedro.readthedocs.io/en/stable/data/\
    data_catalog_yaml_examples.html>`_:

    .. code-block:: yaml

        cars:
          type: tracking.MetricsDataset
          filepath: data/09_tracking/cars.json

    Example usage for the
    `Python API <https://kedro.readthedocs.io/en/stable/data/\
    advanced_data_catalog_usage.html>`_:
    ::

        >>> from kedro_datasets.tracking import MetricsDataset
        >>>
        >>> data = {'col1': 1, 'col2': 0.23, 'col3': 0.002}
        >>>
        >>> dataset = MetricsDataset(filepath="test.json")
        >>> dataset.save(data)

    """

    versioned = True

    def _load(self) -> NoReturn:
        raise DatasetError(f"Loading not supported for '{self.__class__.__name__}'")

    def _save(self, data: Dict[str, float]) -> None:
        """Converts all values in the data from a ``MetricsDataset`` to float to make sure
        they are numeric values which can be displayed in Kedro Viz and then saves the dataset.
        """
        try:
            for key, value in data.items():
                data[key] = float(value)
        except ValueError as exc:
            raise DatasetError(
                f"The MetricsDataset expects only numeric values. {exc}"
            ) from exc

        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            json.dump(data, fs_file, **self._save_args)

        self._invalidate_cache()


_DEPRECATED_CLASSES = {"MetricsDataSet": MetricsDataset}


def __getattr__(name):
    if name in _DEPRECATED_CLASSES:
        alias = _DEPRECATED_CLASSES[name]
        warnings.warn(
            f"{repr(name)} has been renamed to {repr(alias.__name__)}, "
            f"and the alias will be removed in Kedro-Datasets 2.0.0",
            KedroDeprecationWarning,
            stacklevel=2,
        )
        return alias
    raise AttributeError(f"module {repr(__name__)} has no attribute {repr(name)}")
