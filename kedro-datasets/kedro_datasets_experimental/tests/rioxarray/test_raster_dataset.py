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
        np.random.rand(10, 100, 100),  # 10 bands, 100x100 pixels
        dims=("band", "y", "x"),
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

@pytest.fixture
def synthetic_dataset(tmp_path, synthetic_xarray):
    """Create a RasterDataset from the synthetic xarray.DataArray."""
    file_path = tmp_path / "synthetic.nc"
    synthetic_xarray.to_netcdf(file_path)
    return RasterDataset(filepath=file_path.as_posix())

class TestRasterDataset:
    def test_load_cog_geotiff(self, cog_geotiff_dataset):
        """Test saving and reloading the data set."""
        loaded_xr = cog_geotiff_dataset.load()
        assert isinstance(loaded_xr.rio.crs, CRS)
        assert isinstance(loaded_xr, xr.DataArray)
        assert loaded_xr.shape == (1, 500, 500)
        assert loaded_xr.dims == ("band", "y", "x")

    def test_load_synthetic_nc(self, synthetic_dataset):
        """Test loading a synthetic dataset with a CRS."""
        loaded_xr = synthetic_dataset.load()
        assert isinstance(loaded_xr.rio.crs, CRS)
        assert isinstance(loaded_xr, xr.DataArray)

    def test_exists(self, tmp_path, synthetic_xarray):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
        assert not dataset.exists()
        dataset.save(synthetic_xarray)
        assert dataset.exists()

    def test_save_and_load_geotiff(self, cog_geotiff_dataset, cog_xarray):
        """Test saving and reloading the data set."""
        cog_geotiff_dataset.save(cog_xarray)
        reloaded = cog_geotiff_dataset.load()
        assert reloaded.shape == cog_xarray.shape
        assert reloaded.dims == cog_xarray.dims
        #assert reloaded.equals(cog_xarray)

    def test_save_and_load_synthetic(self, synthetic_dataset, synthetic_xarray):
        """Test saving and reloading the synthetic dataset."""
        synthetic_dataset.save(synthetic_xarray)
        reloaded = synthetic_dataset.load()
        #assert reloaded.equals(synthetic_xarray)


    def test_load_missing_file(self, tmp_path):
        """Check the error when trying to load missing file."""
        dataset = RasterDataset(filepath=str(tmp_path / "tmp.tif"))
        assert not dataset._exists(), "File unexpectedly exists"
        pattern = r"Failed while loading data from data set RasterDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            dataset.load()