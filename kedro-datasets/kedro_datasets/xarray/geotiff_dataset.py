"""GeoTiff loads and saves data to a local geoiff file. The
underlying functionality is supported by rioxarray and xarray. A read geotiff file
returns a xarray.DataArray object.
"""

from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any, Dict

import fsspec
import rioxarray as rxr
import xarray
from kedro.io.core import Version, get_filepath_str, get_protocol_and_path

from kedro_datasets._io import AbstractVersionedDataset, DatasetError


class GeoTiffDataset(AbstractVersionedDataset[xarray.DataArray, xarray.DataArray]):
    def __init__(self, filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None):
        """Creates a new instance of ``GeoTiffDataset`` pointing to a concrete
        *.tiff or *.tif file with geospatial data.

        Args:
            filepath: Filepath in POSIX format to a geotiff file.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: rioxarray options for loading geotiff files.
                Here you can find all available arguments:
                https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray-open-rasterio
                All defaults are preserved.
            save_args: rioxarray options for saving to raster file in geotiff format.
                Here you can find all available arguments:
                https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray.raster_dataset.RasterDataset.to_raster
                All defaults are preserved, but "index", which is set to False.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            fs_args: Extra arguments to pass into underlying filesystem class constructor
                (e.g. `{"project": "my-project"}` for ``GCSFileSystem``).
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

        # Handle default load and save arguments
        self._load_args = deepcopy(self.DEFAULT_LOAD_ARGS)
        if load_args is not None:
            self._load_args.update(load_args)
        self._save_args = deepcopy(self.DEFAULT_SAVE_ARGS)
        if save_args is not None:
            self._save_args.update(save_args)

    def _load(self) -> xarray.DataArray:
        load_path = self._get_load_path().as_posix()
        return rxr.open_rasterio(load_path, **self._load_args)

    def _save(self, data: xarray.DataArray) -> None:
        save_path = self._get_save_path()
        if self._filepath.suffix in [".tif", ".tiff"]:
            data.to_raster(save_path.as_posix(), **self._save_args)
        else:
            raise ValueError("expecting .tif or .tiff file suffix")

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)

    def _describe(self) -> Dict[str, Any]:
        return {
            "filepath": self._filepath,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }
