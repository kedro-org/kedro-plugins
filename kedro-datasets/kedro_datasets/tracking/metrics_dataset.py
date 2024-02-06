"""``MetricsDataset`` saves data to a JSON file using an underlying
filesystem (e.g.: local, S3, GCS). It uses native json to handle the JSON file.
The ``MetricsDataset`` is part of Kedro Experiment Tracking. The dataset is versioned by default
and only takes metrics of numeric values.
"""
import json
from typing import NoReturn

from kedro.io.core import DatasetError, get_filepath_str

from kedro_datasets._typing import MetricsTrackingPreview
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

    .. code-block:: pycon

        >>> from kedro_datasets.tracking import MetricsDataset
        >>>
        >>> data = {"col1": 1, "col2": 0.23, "col3": 0.002}
        >>>
        >>> dataset = MetricsDataset(filepath=tmp_path / "test.json")
        >>> dataset.save(data)

    """

    versioned = True

    def _load(self) -> NoReturn:
        raise DatasetError(f"Loading not supported for '{self.__class__.__name__}'")

    def _save(self, data: dict[str, float]) -> None:
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

    def preview(self) -> MetricsTrackingPreview:
        "Load the Metrics tracking dataset used in Kedro-viz experiment tracking"
        load_path = get_filepath_str(self._get_load_path(), self._protocol)

        with self._fs.open(load_path, **self._fs_open_args_load) as fs_file:
            return json.load(fs_file)
