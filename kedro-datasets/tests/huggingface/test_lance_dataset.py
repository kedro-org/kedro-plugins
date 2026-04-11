from pathlib import PurePosixPath

import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.lance_dataset import LanceDataset


@pytest.fixture
def path_lance(tmp_path):
    return (tmp_path / "test.lance").as_posix()


@pytest.fixture
def lance_dataset(path_lance):
    return LanceDataset(path=path_lance)


class TestLanceDataset:
    def test_save_dataset_raises(self, lance_dataset, dataset):
        with pytest.raises(
            DatasetError, match="Saving in lance format is not supported"
        ):
            lance_dataset.save(dataset)

    def test_save_dataset_dict_raises(self, lance_dataset, dataset_dict):
        with pytest.raises(
            DatasetError, match="Saving in lance format is not supported"
        ):
            lance_dataset.save(dataset_dict)

    def test_save_invalid_type(self, lance_dataset):
        pattern = r"LanceDataset only supports"
        with pytest.raises(DatasetError, match=pattern):
            lance_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/data.lance", S3FileSystem),
            ("file:///tmp/data.lance", LocalFileSystem),
            ("/tmp/data.lance", LocalFileSystem),
            ("gcs://bucket/data.lance", GCSFileSystem),
            ("https://example.com/data.lance", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        ds = LanceDataset(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test.lance"
        ds = LanceDataset(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)

    def test_exists_when_missing(self, lance_dataset):
        assert not lance_dataset.exists()

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            LanceDataset(
                path="https://example.com/data.lance",
                version=Version(None, None),
            )
