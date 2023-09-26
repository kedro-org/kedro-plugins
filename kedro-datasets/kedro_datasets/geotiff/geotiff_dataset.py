"""GeoTiff loads and saves data to a local geoiff file. The
underlying functionality is supported by rioxarray and xarray. A read geotiff file
returns a xarray.DataArray object.
"""

import rioxarray as rxr
import xarray

from typing import Any, Dict
from pathlib import PurePosixPath

from kedro.io import AbstractVersionedDataSet, Version
from kedro.io.core import get_filepath_str, get_protocol_and_path

import fsspec


class GeoTiff(AbstractVersionedDataSet):
    def __init__(self, filepath: str, version: Version = None):
        """Creates a new instance of Shape to load / save image data for given filepath.

        Args:
            filepath: The location of the image file to load / save data.
            version: The version of the dataset being saved and loaded.
        """
        protocol, path = get_protocol_and_path(filepath)
        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol)
        self._format = "GEOTIFF"

        super().__init__(
            filepath=PurePosixPath(path),
            version=version,
            exists_function=self._fs.exists,
            glob_function=self._fs.glob,
        )

    def _load(self) -> xarray.DataArray:
        """Loads data GeoTiff.

        Returns:
            Data from the data as a rio xarray
        """
        load_path = self._get_load_path().as_posix()
        return rxr.open_rasterio(load_path)

    def _save(self, data: xarray.DataArray) -> None:
        """Saves Geotiff data to the specified filepath."""
        save_path = self._get_save_path()
        if self._filepath.suffix in [".tif", ".tiff"]:
            data.rio.to_raster(save_path.as_posix())
        else:
            raise ValueError("expecting .tif or .tiff file suffix")

    def _describe(self) -> Dict[str, Any]:
        """Returns a dict that describes the attributes of the dataset."""
        return dict(
            filepath=self._filepath,
            version=self._version,
            protocol=self._protocol,
        )
