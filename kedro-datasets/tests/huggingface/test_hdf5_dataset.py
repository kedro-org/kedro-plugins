from pathlib import PurePosixPath

import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.hdf5_dataset import HDF5Dataset


@pytest.fixture
def path_hdf5(tmp_path):
    return (tmp_path / "test.h5").as_posix()


@pytest.fixture
def hdf5_dataset(path_hdf5):
    return HDF5Dataset(path=path_hdf5)


class TestHDF5Dataset:
    def test_save_dataset_raises(self, hdf5_dataset, dataset):
        with pytest.raises(
            DatasetError, match="Saving in hdf5 format is not supported"
        ):
            hdf5_dataset.save(dataset)

    def test_save_dataset_dict_raises(self, hdf5_dataset, dataset_dict):
        with pytest.raises(
            DatasetError, match="Saving in hdf5 format is not supported"
        ):
            hdf5_dataset.save(dataset_dict)

    def test_save_invalid_type(self, hdf5_dataset):
        pattern = r"HDF5Dataset only supports"
        with pytest.raises(DatasetError, match=pattern):
            hdf5_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/data.h5", S3FileSystem),
            ("file:///tmp/data.h5", LocalFileSystem),
            ("/tmp/data.h5", LocalFileSystem),
            ("gcs://bucket/data.h5", GCSFileSystem),
            ("https://example.com/data.h5", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        ds = HDF5Dataset(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test.h5"
        ds = HDF5Dataset(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)

    def test_exists_when_missing(self, hdf5_dataset):
        assert not hdf5_dataset.exists()

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            HDF5Dataset(
                path="https://example.com/data.h5",
                version=Version(None, None),
            )
