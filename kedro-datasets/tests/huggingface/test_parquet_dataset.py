from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.parquet_dataset import ParquetDataset


@pytest.fixture
def path_parquet(tmp_path):
    return (tmp_path / "test.parquet").as_posix()


@pytest.fixture
def path_parquet_dir(tmp_path):
    return (tmp_path / "test_parquet_dd").as_posix()


@pytest.fixture
def parquet_dataset(path_parquet, save_args, load_args, fs_args):
    return ParquetDataset(
        path=path_parquet,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def parquet_dataset_dir(path_parquet_dir):
    return ParquetDataset(path=path_parquet_dir)


@pytest.fixture
def versioned_parquet_dataset(path_parquet, load_version, save_version):
    return ParquetDataset(
        path=path_parquet, version=Version(load_version, save_version)
    )


class TestParquetDataset:
    def test_save_and_load_dataset(self, parquet_dataset, dataset):
        """A single-file load returns a Dataset (auto-unwrapped)."""
        parquet_dataset.save(dataset)
        reloaded = parquet_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_with_split(self, path_parquet, dataset):
        """With split in load_args, the explicit split is respected."""
        ds = ParquetDataset(path=path_parquet, load_args={"split": "train"})
        ds.save(dataset)
        reloaded = ds.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, parquet_dataset_dir, dataset_dict):
        parquet_dataset_dir.save(dataset_dict)
        reloaded = parquet_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(self, parquet_dataset, iterable_dataset):
        parquet_dataset.save(iterable_dataset)
        reloaded = parquet_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, parquet_dataset_dir, iterable_dataset_dict
    ):
        parquet_dataset_dir.save(iterable_dataset_dict)
        reloaded = parquet_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        assert reloaded["train"].to_dict() == {
            "col1": [1, 2],
            "col2": ["a", "b"],
        }

    def test_exists(self, parquet_dataset, dataset):
        assert not parquet_dataset.exists()
        parquet_dataset.save(dataset)
        assert parquet_dataset.exists()

    def test_load_missing_dataset(self, parquet_dataset):
        pattern = r"Failed while loading data from dataset kedro_datasets.huggingface.parquet_dataset.ParquetDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            parquet_dataset.load()

    def test_save_invalid_type(self, parquet_dataset):
        pattern = r"ParquetDataset only supports"
        with pytest.raises(DatasetError, match=pattern):
            parquet_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/data.parquet", S3FileSystem),
            ("file:///tmp/data.parquet", LocalFileSystem),
            ("/tmp/data.parquet", LocalFileSystem),
            ("gcs://bucket/data.parquet", GCSFileSystem),
            ("https://example.com/data.parquet", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        ds = ParquetDataset(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_pathlike_path(self, tmp_path, dataset):
        path = tmp_path / "test_pathlike.parquet"
        ds = ParquetDataset(path=path)
        ds.save(dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test.parquet"
        ds = ParquetDataset(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestParquetDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        path = "test.parquet"
        ds = ParquetDataset(path=path)
        ds_versioned = ParquetDataset(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "ParquetDataset" in str(ds_versioned)

    def test_save_and_load(self, versioned_parquet_dataset, dataset):
        versioned_parquet_dataset.save(dataset)
        reloaded = versioned_parquet_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_no_versions(self, versioned_parquet_dataset):
        pattern = r"Did not find any versions for kedro_datasets.huggingface.parquet_dataset.ParquetDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.load()

    def test_exists(self, versioned_parquet_dataset, dataset):
        assert not versioned_parquet_dataset.exists()
        versioned_parquet_dataset.save(dataset)
        assert versioned_parquet_dataset.exists()

    def test_prevent_overwrite(self, versioned_parquet_dataset, dataset):
        versioned_parquet_dataset.save(dataset)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.huggingface.parquet_dataset.ParquetDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_parquet_dataset.save(dataset)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            ParquetDataset(
                path="https://example.com/data.parquet",
                version=Version(None, None),
            )
