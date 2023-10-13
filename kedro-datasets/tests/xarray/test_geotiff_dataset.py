from pathlib import Path

import numpy as np
import pytest
import rioxarray
import xarray as xr
from kedro.io.core import Version

from kedro_datasets._io import DatasetError
from kedro_datasets.xarray import GeoTiffDataset


@pytest.fixture
def cog_file_path() -> str:
    cog_file_path = Path(__file__).parent / "cog.tif"
    return cog_file_path.as_posix()


@pytest.fixture
def cog_xarray(cog_file_path) -> xr.DataArray:
    return rioxarray.open_rasterio(cog_file_path)


@pytest.fixture
def cog_geotiff_dataset(cog_file_path, save_args) -> GeoTiffDataset:
    return GeoTiffDataset(filepath=cog_file_path, save_args=save_args)


@pytest.fixture
def filepath_geotiff(tmp_path):
    return (tmp_path / "test.tiff").as_posix()


@pytest.fixture
def geotiff_dataset(filepath_geotiff, save_args):
    return GeoTiffDataset(filepath=filepath_geotiff, save_args=save_args)


def test_load(cog_geotiff_dataset):
    """Test saving and reloading the data set."""
    loaded_tiff = cog_geotiff_dataset.load()
    assert isinstance(loaded_tiff, xr.DataArray)
    assert loaded_tiff.shape == (1, 500, 500)
    assert loaded_tiff.dims == ("band", "y", "x")


def test_exists(geotiff_dataset, cog_xarray):
    """Test `exists` method invocation for both existing and
    nonexistent data set."""
    assert not geotiff_dataset.exists()
    geotiff_dataset.save(cog_xarray)
    assert geotiff_dataset.exists()


def test_save_and_load(geotiff_dataset, cog_xarray):
    """Test saving and reloading the data set."""
    geotiff_dataset.save(cog_xarray)
    reloaded = geotiff_dataset.load()
    assert reloaded.shape == cog_xarray.shape
    assert reloaded.dims == cog_xarray.dims
    assert reloaded.equals(cog_xarray)


def test_example(tmp_path):
    data = xr.DataArray(
        np.random.randn(2, 3, 2),
        dims=("band", "y", "x"),
        coords={"band": [1, 2], "y": [0.5, 1.5, 2.5], "x": [0.5, 1.5]},
    )
    # Add spatial coordinates and CRS information
    data = data.rio.write_crs("epsg:4326")
    data = data.rio.set_spatial_dims("x", "y")
    dataset = GeoTiffDataset(filepath=tmp_path.joinpath("test.tif").as_posix())
    dataset.save(data)
    reloaded = dataset.load()
    xr.testing.assert_allclose(data, reloaded, rtol=1e-5)
