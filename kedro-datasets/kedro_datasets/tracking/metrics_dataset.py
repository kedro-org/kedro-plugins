"""``MetricsDataSet`` saves data to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
The ``MetricsDataSet`` is part of Kedro Experiment Tracking. The dataset is versioned by default
and only takes metrics of numeric values.
"""
import json
from typing import Dict, NoReturn

from kedro.io.core import DataSetError, get_filepath_str

from kedro_datasets.json import JSONDataSet


class MetricsDataSet(JSONDataSet):
    """``MetricsDataSet`` saves data to a JSON file using an underlying
    filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file. The
    ``MetricsDataSet`` is part of Kedro Experiment Tracking. The dataset is write-only,
    it is versioned by default and only takes metrics of numeric values.

Example adding a catalog entry with
    `YAML API
    <https://kedro.readthedocs.io/en/stable/data/\
        data_catalog.html#use-the-data-catalog-with-the-yaml-api>`_:

    .. code-block:: yaml

        >>> cars:
        >>>   type: metrics.MetricsDataSet
        >>>   filepath: data/09_tracking/cars.json

    Example using Python API:
    ::

        >>> from kedro_datasets.tracking import MetricsDataSet
        >>>
        >>> data = {'col1': 1, 'col2': 0.23, 'col3': 0.002}
        >>>
        >>> data_set = MetricsDataSet(filepath="test.json")
        >>> data_set.save(data)

    """

    versioned = True

    def _load(self) -> NoReturn:
        raise DataSetError(f"Loading not supported for '{self.__class__.__name__}'")

    def _save(self, data: Dict[str, float]) -> None:
        """Converts all values in the data from a ``MetricsDataSet`` to float to make sure
        they are numeric values which can be displayed in Kedro Viz and then saves the dataset.
        """
        try:
            for key, value in data.items():
                data[key] = float(value)
        except ValueError as exc:
            raise DataSetError(
                f"The MetricsDataSet expects only numeric values. {exc}"
            ) from exc

        save_path = get_filepath_str(self._get_save_path(), self._protocol)

        with self._fs.open(save_path, **self._fs_open_args_save) as fs_file:
            json.dump(data, fs_file, **self._save_args)

        self._invalidate_cache()
