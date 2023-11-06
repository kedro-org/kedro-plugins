"""``MatlabDataset`` loads/saves data from/to a Matlab file using an underlying
filesystem ?(e.g.: local, S3, GCS)?. The underlying functionality is supported by
the specified backend library passed in (defaults to the ``matlab`` library), so it
supports all allowed options for loading and saving matlab files.
"""
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
import numpy as np
from scipy import io
from kedro.io.core import get_filepath_str, get_protocol_and_path

from kedro_datasets import KedroDeprecationWarning
from kedro_datasets._io import AbstractDataSet, DatasetError


class MatlabDataSet(AbstractDataSet[np.ndarray, np.ndarray]):
    """`MatlabDataSet` loads and saves data from/to a MATLAB file using scipy.io.

    Example:
    ::
        >>> MatlabDataSet(filepath='/data/my_data.mat')
    """

    def __init__(self, filepath: str,
                 fs_args: Dict[str, Any] = None):
        """Creates a new instance of MatlabDataSet to load and save data from/to a MATLAB file.

        Args:
            filepath: The location of the MATLAB file to load and save data.
        """
        _fs_args = deepcopy(fs_args) or {}

        protocol, path = get_protocol_and_path(filepath)
        if protocol == "file":
            _fs_args.setdefault("auto_mkdir", True)
        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol)
        super().__init__(filepath=PurePosixPath(path))

    def _load(self) -> np.ndarray:
        """Loads data from the MATLAB file.

        Returns:
            Data from the MATLAB file as a numpy array
        """
        load_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(load_path, mode="rb") as f:
            data = io.loadmat(f)
            # Access the specific variable in the .mat file, e.g., data['variable_name']
            return data

    def _save(self, data: np.ndarray) -> None:
        """Saves data to the specified MATLAB file."""
        save_path = get_filepath_str(self._filepath, self._protocol)
        with self._fs.open(save_path, mode="wb") as f:
            io.savemat(f, {'data': data})

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(filepath=self._filepath, protocol=self._protocol)

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)



