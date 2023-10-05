from pathlib import Path

import pytest
import xarray as xr
from kedro.io.core import Version

from kedro_datasets.xarray import GeoTiffDataset


@pytest.fixture
def cog_file_path() -> str:
    cog_file_path = Path(__file__).parent / "data" / "avalanches.tif"
    return cog_file_path.as_posix()


@pytest.fixture
def geotiff_dataset(cog_file_path, save_args, fs_args) -> GeoTiffDataset:
    return GeoTiffDataset(filepath=cog_file_path, save_args=save_args)


@pytest.fixture
def versioned_geotiff_dataset(
    filepath_yaml, load_version, save_version
) -> GeoTiffDataset:
    return GeoTiffDataset(
        filepath=filepath_yaml, version=Version(load_version, save_version)
    )


def test_save_and_load(geotiff_dataset):
    """Test saving and reloading the data set."""
    loaded_tiff = geotiff_dataset.load()
    assert isinstance(loaded_tiff, xr.DataArray)

def test_the_test():
    assert True