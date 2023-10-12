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
    """``GeoTiffDataset``  loads and saves geotiff files and reads them as xarray
    DataArrays.


    .. code-block:: yaml

        avalanches:
          type: xarray.GeoTiffDataset
          filepath: avalanches.tif

    Example usage for the
        `Python API <https://kedro.readthedocs.io/en/stable/data/\
        advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.xarray import GeoTiffDataset
        >>> import xarray as xr
        >>> import numpy as np
        >>>
        >>> data = xr.DataArray(
                np.random.randn(2, 3, 2),
                dims=("band", "y", "x"),
                coords={"band": [1, 2], "y": [0.5, 1.5, 2.5], "x": [0.5, 1.5]},
            )
        >>> data = data.rio.write_crs("epsg:4326")
        >>> data = data.rio.set_spatial_dims("x", "y")
        >>> dataset = GeoTiffDataset(filepath="test.tif")
        >>> dataset.save(data)
        >>> reloaded = dataset.load()
        >>> xr.testing.assert_allclose(data, reloaded, rtol=1e-5)

    """

    DEFAULT_LOAD_ARGS: Dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: Dict[str, Any] = {}

    def __init__(
        self,
        filepath: str,
        load_args: Dict[str, Any] = None,
        save_args: Dict[str, Any] = None,
        version: Version = None,
    ):
        """Creates a new instance of ``GeoTiffDataset`` pointing to a concrete
        tiff or tif file with geospatial data.

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
        protocol, path = get_protocol_and_path(filepath, version)
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
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        data.rio.to_raster(save_path, **self._save_args)
        self._fs.invalidate_cache(save_path)

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

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)
