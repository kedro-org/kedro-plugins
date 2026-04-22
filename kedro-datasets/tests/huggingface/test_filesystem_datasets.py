import re
from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.csv_dataset import CSVDataset
from kedro_datasets.huggingface.json_dataset import JSONDataset
from kedro_datasets.huggingface.parquet_dataset import ParquetDataset

FORMATS = [
    pytest.param((CSVDataset, ".csv"), id="csv"),
    pytest.param((JSONDataset, ".json"), id="json"),
    pytest.param((ParquetDataset, ".parquet"), id="parquet"),
]

PROTOCOLS = [
    ("s3://bucket/data", S3FileSystem),
    ("file:///tmp/data", LocalFileSystem),
    ("/tmp/data", LocalFileSystem),
    ("gcs://bucket/data", GCSFileSystem),
    ("https://example.com/data", HTTPFileSystem),
]


def _qualname(cls) -> str:
    return re.escape(f"{cls.__module__}.{cls.__name__}")


@pytest.fixture(params=FORMATS)
def fmt(request):
    return request.param


@pytest.fixture
def dataset_cls(fmt):
    return fmt[0]


@pytest.fixture
def extension(fmt):
    return fmt[1]


@pytest.fixture
def path_file(tmp_path, extension):
    return (tmp_path / f"test{extension}").as_posix()


@pytest.fixture
def path_dir(tmp_path):
    return (tmp_path / "test_dd").as_posix()


@pytest.fixture
def fs_dataset(dataset_cls, path_file, save_args, load_args, fs_args):
    return dataset_cls(
        path=path_file,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def fs_dataset_dir(dataset_cls, path_dir):
    return dataset_cls(path=path_dir)


@pytest.fixture
def versioned_fs_dataset(dataset_cls, path_file, load_version, save_version):
    return dataset_cls(path=path_file, version=Version(load_version, save_version))


class TestFilesystemDataset:
    def test_save_and_load_dataset(self, fs_dataset, dataset):
        """A single-file load returns a Dataset (auto-unwrapped)."""
        fs_dataset.save(dataset)
        reloaded = fs_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_with_split(self, dataset_cls, path_file, dataset):
        """With split in load_args, the explicit split is respected."""
        ds = dataset_cls(path=path_file, load_args={"split": "train"})
        ds.save(dataset)
        reloaded = ds.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, fs_dataset_dir, dataset_dict):
        fs_dataset_dir.save(dataset_dict)
        reloaded = fs_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(self, fs_dataset, iterable_dataset):
        fs_dataset.save(iterable_dataset)
        reloaded = fs_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, fs_dataset_dir, iterable_dataset_dict
    ):
        fs_dataset_dir.save(iterable_dataset_dict)
        reloaded = fs_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        assert reloaded["train"].to_dict() == {
            "col1": [1, 2],
            "col2": ["a", "b"],
        }

    def test_exists(self, fs_dataset, dataset):
        assert not fs_dataset.exists()
        fs_dataset.save(dataset)
        assert fs_dataset.exists()

    def test_load_missing_dataset(self, fs_dataset, dataset_cls):
        pattern = (
            rf"Failed while loading data from dataset {_qualname(dataset_cls)}\(.*\)"
        )
        with pytest.raises(DatasetError, match=pattern):
            fs_dataset.load()

    def test_save_invalid_type(self, fs_dataset, dataset_cls):
        pattern = rf"{dataset_cls.__name__} only supports"
        with pytest.raises(DatasetError, match=pattern):
            fs_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize("base_path,instance_type", PROTOCOLS)
    def test_protocol_usage(self, dataset_cls, extension, base_path, instance_type):
        path = f"{base_path}{extension}"
        ds = dataset_cls(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_pathlike_path(self, dataset_cls, tmp_path, extension, dataset):
        path = tmp_path / f"test_pathlike{extension}"
        ds = dataset_cls(path=path)
        ds.save(dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_catalog_release(self, dataset_cls, extension, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = f"test{extension}"
        ds = dataset_cls(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestFilesystemDatasetVersioned:
    def test_version_str_repr(self, dataset_cls, extension, load_version, save_version):
        path = f"test{extension}"
        ds = dataset_cls(path=path)
        ds_versioned = dataset_cls(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert dataset_cls.__name__ in str(ds_versioned)

    def test_save_and_load(self, versioned_fs_dataset, dataset):
        versioned_fs_dataset.save(dataset)
        reloaded = versioned_fs_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_no_versions(self, versioned_fs_dataset, dataset_cls):
        pattern = rf"Did not find any versions for {_qualname(dataset_cls)}\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_fs_dataset.load()

    def test_exists(self, versioned_fs_dataset, dataset):
        assert not versioned_fs_dataset.exists()
        versioned_fs_dataset.save(dataset)
        assert versioned_fs_dataset.exists()

    def test_prevent_overwrite(self, versioned_fs_dataset, dataset_cls, dataset):
        versioned_fs_dataset.save(dataset)
        pattern = (
            rf"Save path \'.+\' for {_qualname(dataset_cls)}\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_fs_dataset.save(dataset)

    def test_http_filesystem_no_versioning(self, dataset_cls, extension):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            dataset_cls(
                path=f"https://example.com/data{extension}",
                version=Version(None, None),
            )
