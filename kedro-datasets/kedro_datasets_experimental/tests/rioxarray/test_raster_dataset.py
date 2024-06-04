from pathlib import Path

import numpy as np
import pytest
import rioxarray
import xarray as xr
from kedro.io import DatasetError
from rasterio.crs import CRS

from kedro_datasets_experimental.rioxarray.raster_dataset import RasterDataset


@pytest.fixture
def cog_file_path() -> str:
    cog_file_path = Path(__file__).parent / "cog.tif"
    return cog_file_path.as_posix()

@pytest.fixture
def multi1_file_path() -> str:
    path = Path(__file__).parent / "test_multi1.tif"
    return path.as_posix()

@pytest.fixture
def multi2_file_path() -> str:
    path = Path(__file__).parent / "test_multi2.tif"
    return path.as_posix()

@pytest.fixture
def synthetic_xarray():
    """Create a synthetic xarray.DataArray with CRS information."""
    data = xr.DataArray(
        np.random.rand(100, 100),
        dims=("y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data

@pytest.fixture
def synthetic_xarray_multiband():
    """Create a synthetic xarray.DataArray with CRS information."""
    data = xr.DataArray(
        np.random.rand(10, 100, 100),
        dims=("band", "y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data

@pytest.fixture
def synthetic_xarray_many_vars_no_band():
    """Create a synthetic xarray.DataArray with CRS information."""
    data = xr.DataArray(
        np.random.rand(10, 100, 100),
        dims=("var1","var2","var3","y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data


@pytest.fixture
def cog_xarray(cog_file_path) -> xr.DataArray:
    return rioxarray.open_rasterio(cog_file_path)

@pytest.fixture
def cog_geotiff_dataset(cog_file_path, save_args) -> RasterDataset:
    return RasterDataset(filepath=cog_file_path, save_args=save_args)


def test_load_cog_geotiff(cog_geotiff_dataset):
    """Test loading cloud optimised geotiff reloading the data set."""
    loaded_xr = cog_geotiff_dataset.load()
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    assert loaded_xr.shape == (1, 500, 500)
    assert loaded_xr.dims == ("band", "y", "x")

def test_load_save_multi1(tmp_path,multi1_file_path):
    """Test loading a multiband raster file."""
    dataset = RasterDataset(filepath=multi1_file_path)
    dataset_to = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    loaded_xr = dataset.load()
    band1_data = loaded_xr.sel(band=1)
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    assert len(loaded_xr.band) == 2
    assert loaded_xr.shape == (2, 5, 5)
    assert loaded_xr.dims == ("band", "y", "x")
    assert np.isclose(band1_data.values.std(), 0.015918046)
    dataset_to.save(loaded_xr)
    reloaded_xr = dataset_to.load()
    assert (loaded_xr.values == reloaded_xr.values).all()

def test_load_no_crs(multi2_file_path):
    """Test loading a multiband raster file."""
    dataset = RasterDataset(filepath=multi2_file_path)
    with pytest.raises(DatasetError):
        dataset.load()


def test_exists(tmp_path, synthetic_xarray):
    """Test `exists` method invocation for both existing and
    nonexistent data set."""
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset.exists()
    dataset.save(synthetic_xarray)
    assert dataset.exists()

@pytest.mark.parametrize("xarray_fixture", [
    "synthetic_xarray_multiband",
    "synthetic_xarray",
    "cog_xarray",
])
def test_save_and_load_geotiff(tmp_path, request, xarray_fixture):
    """Test saving and reloading the data set."""
    xarray_data = request.getfixturevalue(xarray_fixture)
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    dataset.save(xarray_data)
    reloaded = dataset.load()
    assert isinstance(reloaded, xr.DataArray)
    assert isinstance(reloaded.rio.crs, CRS)
    assert reloaded.dims == ("band", "y", "x")
    assert reloaded.equals(xarray_data)

def test_save_and_load_geotiff_no_band(tmp_path, synthetic_xarray_many_vars_no_band):
    """this test should fail because the data array has no band dimension"""
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    with pytest.raises(ValueError):
        dataset.save(synthetic_xarray_many_vars_no_band)

def test_load_missing_file(tmp_path):
    """Check the error when trying to load missing file."""
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset._exists(), "File unexpectedly exists"
    pattern = r"Failed while loading data from data set RasterDataset\(.*\)"
    with pytest.raises(DatasetError, match=pattern):
        dataset.load()