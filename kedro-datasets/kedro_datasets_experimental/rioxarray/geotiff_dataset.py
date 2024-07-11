"""GeoTIFFDataset loads geospatial raster data and saves it to a local geoiff file. The
underlying functionality is supported by rioxarray and xarray. A read rasterdata file
returns a xarray.DataArray object.
"""
import logging
from copy import deepcopy
from pathlib import PurePosixPath
from typing import Any

import fsspec
import rasterio
import rioxarray as rxr
import xarray
from kedro.io import AbstractVersionedDataset, DatasetError
from kedro.io.core import Version, get_filepath_str, get_protocol_and_path
from rasterio.crs import CRS
from rasterio.transform import from_bounds

logger = logging.getLogger(__name__)

SUPPORTED_DIMS = [("band", "x", "y"), ("x", "y")]
DEFAULT_NO_DATA_VALUE = -9999
SUPPORTED_FILE_FORMATS = [".tif", ".tiff"]


class GeoTIFFDataset(AbstractVersionedDataset[xarray.DataArray, xarray.DataArray]):
    """``GeoTIFFDataset`` loads and saves rasterdata files and reads them as xarray
    DataArrays. The underlying functionality is supported by rioxarray, rasterio and xarray.

    Reading and writing of single and multiband GeoTIFFs data is supported. There are sanity checks to ensure that a coordinate reference system (CRS) is present.
    Supported dimensions are ("band", "x", "y") and ("x", "y") and xarray.DataArray with other dimension can not be saved to a GeoTIFF file.
    Have a look at netcdf if this is what you need.


    .. code-block:: yaml

        sentinal_data:
          type: rioxarray.GeoTIFFDataset
          filepath: sentinal_data.tif

    Example usage for the
        `Python API <https://kedro.readthedocs.io/en/stable/data/\
        advanced_data_catalog_usage.html>`_:

    .. code-block:: pycon

        >>> from kedro_datasets.rioxarray import GeoTIFFDataset
        >>> import xarray as xr
        >>> import numpy as np
        >>>
        >>> data = xr.DataArray(
        ...     np.random.randn(2, 3, 2),
        ...     dims=("band", "y", "x"),
        ...     coords={"band": [1, 2], "y": [0.5, 1.5, 2.5], "x": [0.5, 1.5]}
        ... )
        >>> data_crs = data.rio.write_crs("epsg:4326")
        >>> data_spatial_dims = data_crs.rio.set_spatial_dims("x", "y")
        >>> dataset = GeoTIFFDataset(filepath="test.tif")
        >>> dataset.save(data_spatial_dims)
        >>> reloaded = dataset.load()
        >>> xr.testing.assert_allclose(data_spatial_dims, reloaded, rtol=1e-5)

    """

    DEFAULT_LOAD_ARGS: dict[str, Any] = {}
    DEFAULT_SAVE_ARGS: dict[str, Any] = {}

    def __init__(  # noqa: PLR0913
        self,
        *,
        filepath: str,
        load_args: dict[str, Any] | None = None,
        save_args: dict[str, Any] | None = None,
        version: Version | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """Creates a new instance of ``GeoTIFFDataset`` pointing to a concrete
        geospatial raster data file.


        Args:
            filepath: Filepath in POSIX format to a rasterdata file.
                The prefix should be any protocol supported by ``fsspec``.
            load_args: rioxarray options for loading rasterdata files.
                Here you can find all available arguments:
                https://corteva.github.io/rioxarray/html/rioxarray.html#rioxarray-open-rasterio
                All defaults are preserved.
            save_args: options for rioxarray for data without the band dimension and rasterio otherwhise.
            version: If specified, should be an instance of
                ``kedro.io.core.Version``. If its ``load`` attribute is
                None, the latest version will be loaded. If its ``save``
                attribute is None, save version will be autogenerated.
            metadata: Any arbitrary metadata.
                This is ignored by Kedro, but may be consumed by users or external plugins.
        """
        protocol, path = get_protocol_and_path(filepath, version)
        self._protocol = protocol
        self._fs = fsspec.filesystem(self._protocol)
        self.metadata = metadata

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

    def _describe(self) -> dict[str, Any]:
        return {
            "filepath": self._filepath,
            "protocol": self._protocol,
            "load_args": self._load_args,
            "save_args": self._save_args,
            "version": self._version,
        }

    def _load(self) -> xarray.DataArray:
        load_path = self._get_load_path().as_posix()
        with rasterio.open(load_path) as data:
            tags = data.tags()
        data = rxr.open_rasterio(load_path, **self._load_args)
        data.attrs.update(tags)
        self._sanity_check(data)
        logger.info(f"found coordinate rerence system {data.rio.crs}")
        return data

    def _save(self, data: xarray.DataArray) -> None:
        self._sanity_check(data)
        save_path = get_filepath_str(self._get_save_path(), self._protocol)
        if not save_path.endswith(tuple(SUPPORTED_FILE_FORMATS)):
            raise ValueError(
                f"Unsupported file format. Supported formats are: {SUPPORTED_FILE_FORMATS}"
            )
        if "band" in data.dims:
            self._save_multiband(data, save_path)
        else:
            data.rio.to_raster(save_path, **self._save_args)
        self._fs.invalidate_cache(save_path)

    def _exists(self) -> bool:
        try:
            load_path = get_filepath_str(self._get_load_path(), self._protocol)
        except DatasetError:
            return False

        return self._fs.exists(load_path)

    def _release(self) -> None:
        super()._release()
        self._invalidate_cache()

    def _invalidate_cache(self) -> None:
        """Invalidate underlying filesystem caches."""
        filepath = get_filepath_str(self._filepath, self._protocol)
        self._fs.invalidate_cache(filepath)

    def _save_multiband(self, data: xarray.DataArray, save_path: str):
        """Saving multiband raster data to a geotiff file."""
        bands_data = [data.sel(band=band) for band in data.band.values]
        transform = from_bounds(
            west=data.x.min(),
            south=data.y.min(),
            east=data.x.max(),
            north=data.y.max(),
            width=data[0].shape[1],
            height=data[0].shape[0],
        )

        nodata_value = (
            data.rio.nodata if data.rio.nodata is not None else DEFAULT_NO_DATA_VALUE
        )
        crs = data.rio.crs

        meta = {
            "driver": "GTiff",
            "height": bands_data[0].shape[0],
            "width": bands_data[0].shape[1],
            "count": len(bands_data),
            "dtype": str(bands_data[0].dtype),
            "crs": crs,
            "transform": transform,
            "nodata": nodata_value,
        }
        with rasterio.open(save_path, "w", **meta) as dst:
            for idx, band in enumerate(bands_data, start=1):
                dst.write(band.data, idx, **self._save_args)

    def _sanity_check(self, data: xarray.DataArray) -> None:
        """Perform sanity checks on the data to ensure it meets the requirements."""
        if not isinstance(data, xarray.DataArray):
            raise NotImplementedError(
                "Currently only supporting xarray.DataArray while saving raster data."
            )

        if not isinstance(data.rio.crs, CRS):
            raise ValueError("Dataset lacks a coordinate reference system.")

        if all(set(data.dims) != set(dims) for dims in SUPPORTED_DIMS):
            raise ValueError(
                f"Data has unsupported dimensions: {data.dims}. Supported dimensions are: {SUPPORTED_DIMS}"
            )
