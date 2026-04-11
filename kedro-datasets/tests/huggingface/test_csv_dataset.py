from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.csv_dataset import CSVDataset


@pytest.fixture
def path_csv(tmp_path):
    return (tmp_path / "test.csv").as_posix()


@pytest.fixture
def path_csv_dir(tmp_path):
    return (tmp_path / "test_csv_dd").as_posix()


@pytest.fixture
def csv_dataset(path_csv, save_args, load_args, fs_args):
    return CSVDataset(
        path=path_csv,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def csv_dataset_dir(path_csv_dir):
    return CSVDataset(path=path_csv_dir)


@pytest.fixture
def versioned_csv_dataset(path_csv, load_version, save_version):
    return CSVDataset(path=path_csv, version=Version(load_version, save_version))


class TestCSVDataset:
    def test_save_and_load_dataset(self, csv_dataset, dataset):
        """A single-file load returns a Dataset (auto-unwrapped)."""
        csv_dataset.save(dataset)
        reloaded = csv_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_with_split(self, path_csv, dataset):
        """With split in load_args, the explicit split is respected."""
        ds = CSVDataset(path=path_csv, load_args={"split": "train"})
        ds.save(dataset)
        reloaded = ds.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, csv_dataset_dir, dataset_dict):
        csv_dataset_dir.save(dataset_dict)
        reloaded = csv_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(self, csv_dataset, iterable_dataset):
        csv_dataset.save(iterable_dataset)
        reloaded = csv_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, csv_dataset_dir, iterable_dataset_dict
    ):
        csv_dataset_dir.save(iterable_dataset_dict)
        reloaded = csv_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        assert reloaded["train"].to_dict() == {
            "col1": [1, 2],
            "col2": ["a", "b"],
        }

    def test_exists(self, csv_dataset, dataset):
        assert not csv_dataset.exists()
        csv_dataset.save(dataset)
        assert csv_dataset.exists()

    def test_load_missing_dataset(self, csv_dataset):
        pattern = r"Failed while loading data from dataset kedro_datasets.huggingface.csv_dataset.CSVDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            csv_dataset.load()

    def test_save_invalid_type(self, csv_dataset):
        pattern = r"CSVDataset only supports"
        with pytest.raises(DatasetError, match=pattern):
            csv_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/data.csv", S3FileSystem),
            ("file:///tmp/data.csv", LocalFileSystem),
            ("/tmp/data.csv", LocalFileSystem),
            ("gcs://bucket/data.csv", GCSFileSystem),
            ("https://example.com/data.csv", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        ds = CSVDataset(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_pathlike_path(self, tmp_path, dataset):
        path = tmp_path / "test_pathlike.csv"
        ds = CSVDataset(path=path)
        ds.save(dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test.csv"
        ds = CSVDataset(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestCSVDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        path = "test.csv"
        ds = CSVDataset(path=path)
        ds_versioned = CSVDataset(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "CSVDataset" in str(ds_versioned)

    def test_save_and_load(self, versioned_csv_dataset, dataset):
        versioned_csv_dataset.save(dataset)
        reloaded = versioned_csv_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_no_versions(self, versioned_csv_dataset):
        pattern = r"Did not find any versions for kedro_datasets.huggingface.csv_dataset.CSVDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_csv_dataset.load()

    def test_exists(self, versioned_csv_dataset, dataset):
        assert not versioned_csv_dataset.exists()
        versioned_csv_dataset.save(dataset)
        assert versioned_csv_dataset.exists()

    def test_prevent_overwrite(self, versioned_csv_dataset, dataset):
        versioned_csv_dataset.save(dataset)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.huggingface.csv_dataset.CSVDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_csv_dataset.save(dataset)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            CSVDataset(
                path="https://example.com/data.csv",
                version=Version(None, None),
            )
