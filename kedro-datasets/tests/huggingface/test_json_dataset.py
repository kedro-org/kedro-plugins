from pathlib import PurePosixPath

import pytest
from datasets import Dataset, DatasetDict
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.huggingface.json_dataset import JSONDataset


@pytest.fixture
def path_json(tmp_path):
    return (tmp_path / "test.json").as_posix()


@pytest.fixture
def path_json_dir(tmp_path):
    return (tmp_path / "test_json_dd").as_posix()


@pytest.fixture
def json_dataset(path_json, save_args, load_args, fs_args):
    return JSONDataset(
        path=path_json,
        save_args=save_args,
        load_args=load_args,
        fs_args=fs_args,
    )


@pytest.fixture
def json_dataset_dir(path_json_dir):
    return JSONDataset(path=path_json_dir)


@pytest.fixture
def versioned_json_dataset(path_json, load_version, save_version):
    return JSONDataset(path=path_json, version=Version(load_version, save_version))


class TestJSONDataset:
    def test_save_and_load_dataset(self, json_dataset, dataset):
        """A single-file load returns a Dataset (auto-unwrapped)."""
        json_dataset.save(dataset)
        reloaded = json_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_with_split(self, path_json, dataset):
        """With split in load_args, the explicit split is respected."""
        ds = JSONDataset(path=path_json, load_args={"split": "train"})
        ds.save(dataset)
        reloaded = ds.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_save_and_load_dataset_dict(self, json_dataset_dir, dataset_dict):
        json_dataset_dir.save(dataset_dict)
        reloaded = json_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        for split in dataset_dict:
            assert reloaded[split].to_dict() == dataset_dict[split].to_dict()

    def test_save_and_load_iterable_dataset(self, json_dataset, iterable_dataset):
        json_dataset.save(iterable_dataset)
        reloaded = json_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == {
            "col1": [1, 2, 3],
            "col2": ["a", "b", "c"],
        }

    def test_save_and_load_iterable_dataset_dict(
        self, json_dataset_dir, iterable_dataset_dict
    ):
        json_dataset_dir.save(iterable_dataset_dict)
        reloaded = json_dataset_dir.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == {"train", "test"}
        assert reloaded["train"].to_dict() == {
            "col1": [1, 2],
            "col2": ["a", "b"],
        }

    def test_exists(self, json_dataset, dataset):
        assert not json_dataset.exists()
        json_dataset.save(dataset)
        assert json_dataset.exists()

    def test_load_missing_dataset(self, json_dataset):
        pattern = r"Failed while loading data from dataset kedro_datasets.huggingface.json_dataset.JSONDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            json_dataset.load()

    def test_save_invalid_type(self, json_dataset):
        pattern = r"JSONDataset only supports"
        with pytest.raises(DatasetError, match=pattern):
            json_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize(
        "path,instance_type",
        [
            ("s3://bucket/data.json", S3FileSystem),
            ("file:///tmp/data.json", LocalFileSystem),
            ("/tmp/data.json", LocalFileSystem),
            ("gcs://bucket/data.json", GCSFileSystem),
            ("https://example.com/data.json", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, path, instance_type):
        ds = JSONDataset(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_pathlike_path(self, tmp_path, dataset):
        path = tmp_path / "test_pathlike.json"
        ds = JSONDataset(path=path)
        ds.save(dataset)
        reloaded = ds.load()
        assert reloaded.to_dict() == dataset.to_dict()

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        path = "test.json"
        ds = JSONDataset(path=path)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path)


class TestJSONDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        path = "test.json"
        ds = JSONDataset(path=path)
        ds_versioned = JSONDataset(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "JSONDataset" in str(ds_versioned)

    def test_save_and_load(self, versioned_json_dataset, dataset):
        versioned_json_dataset.save(dataset)
        reloaded = versioned_json_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == dataset.to_dict()

    def test_no_versions(self, versioned_json_dataset):
        pattern = r"Did not find any versions for kedro_datasets.huggingface.json_dataset.JSONDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.load()

    def test_exists(self, versioned_json_dataset, dataset):
        assert not versioned_json_dataset.exists()
        versioned_json_dataset.save(dataset)
        assert versioned_json_dataset.exists()

    def test_prevent_overwrite(self, versioned_json_dataset, dataset):
        versioned_json_dataset.save(dataset)
        pattern = (
            r"Save path \'.+\' for kedro_datasets.huggingface.json_dataset.JSONDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.save(dataset)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."
        with pytest.raises(DatasetError, match=pattern):
            JSONDataset(
                path="https://example.com/data.json",
                version=Version(None, None),
            )
