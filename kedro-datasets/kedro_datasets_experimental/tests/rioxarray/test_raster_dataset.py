from pathlib import Path

import numpy as np
import pytest
import rioxarray
import xarray as xr
from kedro.io import DatasetError

from kedro_datasets_experimental.rioxarray.raster_dataset import RasterDataset


@pytest.fixture
def cog_file_path() -> str:
    cog_file_path = Path(__file__).parent / "cog.tif"
    return cog_file_path.as_posix()


@pytest.fixture
def cog_xarray(cog_file_path) -> xr.DataArray:
    return rioxarray.open_rasterio(cog_file_path)


@pytest.fixture
def cog_geotiff_dataset(cog_file_path, save_args) -> RasterDataset:
    return RasterDataset(filepath=cog_file_path, save_args=save_args)


@pytest.fixture
def filepath_geotiff(tmp_path):
    return (tmp_path / "test.tiff").as_posix()


@pytest.fixture
def geotiff_dataset(filepath_geotiff, load_args, save_args):
    return RasterDataset(filepath=filepath_geotiff, load_args=load_args, save_args=save_args)


class TestRasterDataset:
    def test_load(self, cog_geotiff_dataset):
        """Test saving and reloading the data set."""
        loaded_tiff = cog_geotiff_dataset.load()
        assert isinstance(loaded_tiff, xr.DataArray)
        assert loaded_tiff.shape == (1, 500, 500)
        assert loaded_tiff.dims == ("band", "y", "x")

    def test_exists(self, geotiff_dataset, cog_xarray):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not geotiff_dataset.exists()
        geotiff_dataset.save(cog_xarray)
        assert geotiff_dataset.exists()

    def test_save_and_load(self, geotiff_dataset, cog_xarray):
        """Test saving and reloading the data set."""
        geotiff_dataset.save(cog_xarray)
        reloaded = geotiff_dataset.load()
        assert reloaded.shape == cog_xarray.shape
        assert reloaded.dims == cog_xarray.dims
        assert reloaded.equals(cog_xarray)

    def test_example(self, tmp_path):
        data = xr.DataArray(
            np.random.randn(2, 3, 2),
            dims=("band", "y", "x"),
            coords={"band": [1, 2], "y": [0.5, 1.5, 2.5], "x": [0.5, 1.5]},
        )
        # Add spatial coordinates and CRS information
        data = data.rio.write_crs("epsg:4326")
        data = data.rio.set_spatial_dims("x", "y")
        dataset = RasterDataset(filepath=tmp_path.joinpath("test.tif").as_posix())
        dataset.save(data)
        reloaded = dataset.load()
        xr.testing.assert_allclose(data, reloaded, rtol=1e-5)

    @pytest.mark.parametrize(
        "load_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_load_extra_params(self, geotiff_dataset, load_args):
        """Test overriding the default load arguments."""
        for key, value in load_args.items():
            assert geotiff_dataset._load_args[key] == value

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, geotiff_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert geotiff_dataset._save_args[key] == value

    def test_load_missing_file(self, geotiff_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set RasterDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            geotiff_dataset.load()
