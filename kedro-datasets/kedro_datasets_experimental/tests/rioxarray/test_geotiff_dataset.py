from pathlib import Path

import numpy as np
import pytest
import rasterio
import xarray as xr
from kedro.io import DatasetError
from rasterio.crs import CRS

from kedro_datasets_experimental.rioxarray.geotiff_dataset import GeoTIFFDataset


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
        np.random.rand(2,3,4, 100, 100),
        dims=("var1","var2","var3","y", "x"),
        coords={"x": np.linspace(0, 100, 100), "y": np.linspace(0, 100, 100)}
    )
    data.rio.write_crs("epsg:4326", inplace=True)
    return data


@pytest.fixture
def cog_geotiff_dataset(cog_file_path, save_args) -> GeoTIFFDataset:
    return GeoTIFFDataset(filepath=cog_file_path, save_args=save_args)


def test_load_cog_geotiff(cog_geotiff_dataset):
    """Test loading cloud optimised geotiff reloading the dataset."""
    loaded_xr = cog_geotiff_dataset.load()
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    assert loaded_xr.shape == (1, 500, 500)
    assert loaded_xr.dims == ("band", "y", "x")

def test_load_save_cog(tmp_path,cog_file_path):
    """Test loading a multiband raster file."""
    dataset = GeoTIFFDataset(filepath=cog_file_path)
    loaded_xr = dataset.load()
    band1_data = loaded_xr.sel(band=1)
    target_file = tmp_path / "tmp22.tif"
    dataset_to = GeoTIFFDataset(filepath=str(target_file))
    dataset_to.save(loaded_xr)
    reloaded_xr = dataset_to.load()
    assert target_file.exists()
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    assert len(loaded_xr.band) == 1
    assert loaded_xr.dims == ("band", "y", "x")
    assert loaded_xr.shape == (1, 500, 500)
    assert np.isclose(band1_data.values.std(), 4688.72624578268)
    assert (loaded_xr.values == reloaded_xr.values).all()



def test_load_save_multi1(tmp_path,multi1_file_path):
    """Test loading a multiband raster file."""
    dataset = GeoTIFFDataset(filepath=multi1_file_path)
    dataset_to = GeoTIFFDataset(filepath=str(tmp_path / "tmp.tif"))
    loaded_xr = dataset.load()
    band1_data = loaded_xr.sel(band=1)
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert isinstance(loaded_xr, xr.DataArray)
    BAND_COUNT = 2
    assert len(loaded_xr.band) == BAND_COUNT
    assert loaded_xr.shape == (BAND_COUNT, 5, 5)
    assert loaded_xr.dims == ("band", "y", "x")
    assert np.isclose(band1_data.values.std(), 0.015918046)
    dataset_to.save(loaded_xr)
    reloaded_xr = dataset_to.load()
    assert (loaded_xr.values == reloaded_xr.values).all()

def test_load_geotiff_with_tags(tmp_path, synthetic_xarray):
    filepath = tmp_path / "test_with_tags.tif"
    tags = {"TAG_KEY": "TAG_VALUE", "ANOTHER_TAG": "ANOTHER_VALUE"}
    with rasterio.open(
        filepath, "w", driver="GTiff", height=100, width=100, count=1, dtype=str(synthetic_xarray.dtype),
        crs="EPSG:4326"
    ) as dst:
        dst.write(synthetic_xarray.values, 1)
        dst.update_tags(**tags)

    dataset = GeoTIFFDataset(filepath=str(filepath))
    loaded_xr = dataset.load()

    assert loaded_xr.attrs["TAG_KEY"] == "TAG_VALUE"
    assert loaded_xr.attrs["ANOTHER_TAG"] == "ANOTHER_VALUE"

    assert isinstance(loaded_xr, xr.DataArray)
    assert isinstance(loaded_xr.rio.crs, CRS)
    assert loaded_xr.shape == (1, 100, 100)

def test_load_no_crs(multi2_file_path):
    """Test loading a multiband raster file."""
    dataset = GeoTIFFDataset(filepath=multi2_file_path)
    with pytest.raises(DatasetError):
        dataset.load()

def test_load_not_tif():
    """Test loading a multiband raster file."""
    dataset = GeoTIFFDataset(filepath="whatever.nc")
    with pytest.raises(DatasetError):
        dataset.load()


def test_exists(tmp_path, synthetic_xarray):
    """Test `exists` method invocation for both existing and
    nonexistent dataset."""
    dataset = GeoTIFFDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset.exists()
    dataset.save(synthetic_xarray)
    assert dataset.exists()

@pytest.mark.parametrize("xarray_fixture", [
    "synthetic_xarray_multiband",
    "synthetic_xarray",
])
def test_save_and_load_geotiff(tmp_path, request, xarray_fixture):
    """Test saving and reloading the dataset."""
    xarray_data = request.getfixturevalue(xarray_fixture)
    dataset = GeoTIFFDataset(filepath=str(tmp_path / "tmp.tif"))
    dataset.save(xarray_data)
    assert dataset.exists()
    reloaded_xr = dataset.load()
    assert isinstance(reloaded_xr, xr.DataArray)
    assert isinstance(reloaded_xr.rio.crs, CRS)
    assert reloaded_xr.dims == ("band", "y", "x")
    assert (xarray_data.values == reloaded_xr.values).all()

def test_save_and_load_geotiff_no_band(tmp_path, synthetic_xarray_many_vars_no_band):
    """this test should fail because the data array has no band dimension"""
    dataset = GeoTIFFDataset(filepath=str(tmp_path / "tmp.tif"))
    with pytest.raises(DatasetError):
        dataset.save(synthetic_xarray_many_vars_no_band)

def test_load_missing_file(tmp_path):
    """Check the error when trying to load missing file."""
    dataset = GeoTIFFDataset(filepath=str(tmp_path / "tmp.tif"))
    assert not dataset._exists(), "File unexpectedly exists"
    pattern = r"Failed while loading data from dataset GeoTIFFDataset\(.*\)"
    with pytest.raises(DatasetError, match=pattern):
        dataset.load()
