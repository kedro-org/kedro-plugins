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
def synthetic_xarray():
    """Create a synthetic xarray.DataArray with CRS information."""
    data = xr.DataArray(
        np.random.rand(100, 100),
        dims=("y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data.to_dataset(name="synthetic")

@pytest.fixture
def synthetic_xarray_multiband():
    """Create a synthetic xarray.DataArray with CRS information."""
    data = xr.DataArray(
        np.random.rand(10, 100, 100),
        dims=("band", "y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data.to_dataset(name="synthetic")

@pytest.fixture
def cog_xarray(cog_file_path) -> xr.DataArray:
    return rioxarray.open_rasterio(cog_file_path)

@pytest.fixture
def cog_geotiff_dataset(cog_file_path, save_args) -> RasterDataset:
    return RasterDataset(filepath=cog_file_path, save_args=save_args)



def test_load_cog_geotiff(cog_geotiff_dataset):
    """Test saving and reloading the data set."""
    loaded_xr = cog_geotiff_dataset.load()
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    assert loaded_xr.shape == (1, 500, 500)
    assert loaded_xr.dims == ("band", "y", "x")

def test_exists(tmp_path, synthetic_xarray):
    """Test `exists` method invocation for both existing and
    nonexistent data set."""
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset.exists()
    dataset.save(synthetic_xarray)
    assert dataset.exists()
@pytest.mark.parametrize("xarray_fixture", [
    "synthetic_xarray",           # Single-band synthetic data
    "cog_xarray",                 # COG file data, may also be multiband depending on the fixture setup
    "synthetic_xarray_multiband"  # Explicitly multiband synthetic data
])
def test_save_and_load_geotiff(tmp_path, request, xarray_fixture):
    """Test saving and reloading the data set."""
    xarray_data = request.getfixturevalue(xarray_fixture)
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    dataset.save(xarray_data)
    reloaded = dataset.load()
    assert isinstance(reloaded, xr.DataArray)
    assert isinstance(reloaded.rio.crs, CRS)
    assert reloaded.equals(xarray_data)

def test_load_missing_file(tmp_path):
    """Check the error when trying to load missing file."""
    dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset._exists(), "File unexpectedly exists"
    pattern = r"Failed while loading data from data set RasterDataset\(.*\)"
    with pytest.raises(DatasetError, match=pattern):
        dataset.load()