from pathlib import Path

import pytest
import rioxarray
from kedro.io.core import Version
from kedro_datasets.xarray import GeoTiffDataset


@pytest.fixture
def cog_file_path():
    return Path(__file__).parent / "cog.tif"

@pytest.fixture
def geotiff_dataset(cog_file_path, save_args, fs_args) -> GeoTiffDataset:
    return GeoTiffDataset(filepath=cog_file_path, save_args=save_args, fs_args=fs_args)

@pytest.fixture
def versioned_geotiff_dataset(filepath_yaml, load_version, save_version) -> GeoTiffDataset:
    return GeoTiffDataset(
        filepath=filepath_yaml, version=Version(load_version, save_version)
    )



def test_save_and_load(self, geotiff_dataset, cog_file_path):
    """Test saving and reloading the data set."""
    loaded_tiff = geotiff_dataset.load()
    assert type(loaded_tiff) == rioxarray.raster_dataset.RasterArray

