import os
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
def kedro_dataset_cls(fmt):
    return fmt[0]


@pytest.fixture
def extension(fmt):
    return fmt[1]


@pytest.fixture
def dataset_dict_data_files(extension):
    return {
        "data": f"data{extension}",
        "labels": f"labels{extension}",
    }


@pytest.fixture
def custom_dataset_dict_data_files(extension):
    return {
        "data": f"my_data{extension}",
        "labels": f"my_labels{extension}",
    }


@pytest.fixture
def path_file(tmp_path, extension):
    return (tmp_path / f"test{extension}").as_posix()


@pytest.fixture
def path_dir(tmp_path):
    return (tmp_path / "test_dd").as_posix()


@pytest.fixture
def load_version():
    return "2019-01-01T23.59.59.999Z"


@pytest.fixture
def save_version():
    return "2019-01-01T23.59.59.999Z"


@pytest.fixture
def versioned_fs_dataset(kedro_dataset_cls, path_file, load_version, save_version):
    return kedro_dataset_cls(
        path=path_file, version=Version(load_version, save_version)
    )


class TestFilesystemDataset:
    def test_save_and_load_dataset(self, hf_dataset, kedro_dataset_cls, path_file):
        """A single-file load returns a DatasetDict with single key "train"."""
        kedro_dataset = kedro_dataset_cls(path=path_file)
        kedro_dataset.save(hf_dataset)
        reloaded = kedro_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert "train" in reloaded
        assert reloaded["train"].to_dict() == hf_dataset.to_dict()

    def test_save_and_load_dataset_with_split(
        self, hf_dataset, kedro_dataset_cls, path_file
    ):
        """With split in load_args, the explicit split is respected."""
        kedro_dataset = kedro_dataset_cls(path=path_file, load_args={"split": "train"})
        kedro_dataset.save(hf_dataset)
        reloaded = kedro_dataset.load()
        assert isinstance(reloaded, Dataset)
        assert reloaded.to_dict() == hf_dataset.to_dict()

    def test_build_data_files(
        self, kedro_dataset_cls, path_dir, dataset_dict_data_files
    ):
        kedro_dataset = kedro_dataset_cls(
            path=path_dir, data_files=dataset_dict_data_files
        )

        built_data_files = kedro_dataset._build_data_files()

        for split, filename in dataset_dict_data_files.items():
            assert split in built_data_files
            assert built_data_files[split] == os.path.join(path_dir, filename)

    def test_save_and_load_dataset_dict(
        self, dataset_dict, kedro_dataset_cls, path_dir, dataset_dict_data_files
    ):
        kedro_dataset = kedro_dataset_cls(
            path=path_dir, data_files=dataset_dict_data_files
        )
        kedro_dataset.save(dataset_dict)

        reloaded = kedro_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == dataset_dict_data_files.keys()
        for key in dataset_dict_data_files.keys():
            assert reloaded[key].to_dict() == dataset_dict[key].to_dict()

    def test_save_and_load_dataset_dict_with_custom_data_files(
        self,
        dataset_dict,
        kedro_dataset_cls,
        path_dir,
        custom_dataset_dict_data_files,
    ):
        kedro_dataset = kedro_dataset_cls(
            path=path_dir, data_files=custom_dataset_dict_data_files
        )
        kedro_dataset.save(dataset_dict)

        for filename in custom_dataset_dict_data_files.values():
            assert os.path.exists(os.path.join(path_dir, filename))

        reloaded = kedro_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert set(reloaded.keys()) == custom_dataset_dict_data_files.keys()
        for key in custom_dataset_dict_data_files.keys():
            assert reloaded[key].to_dict() == dataset_dict[key].to_dict()

    def test_save_dataset_dict_mismatched_data_files(
        self, dataset_dict, kedro_dataset_cls, path_dir, extension
    ):
        """Saving a DatasetDict whose split names don't match data_files keys raises DatasetError."""
        kedro_dataset = kedro_dataset_cls(
            path=path_dir,
            # In the test fixture, we expect "data" and "labels". Not "train" and "test".
            data_files={
                "train": f"train{extension}",
                "test": f"test{extension}",
            },
        )
        with pytest.raises(DatasetError, match=r"do not match"):
            kedro_dataset.save(dataset_dict)

    @pytest.mark.parametrize("args_name", ["load_args", "save_args"])
    @pytest.mark.parametrize("top_level_data_files", [True, False])
    def test_data_files_in_args_raises_dataset_error(
        self,
        kedro_dataset_cls,
        path_dir,
        dataset_dict_data_files,
        args_name,
        top_level_data_files,
    ):
        args = {
            "path": path_dir,
            args_name: {"data_files": dataset_dict_data_files},
        }
        if top_level_data_files:
            args["data_files"] = dataset_dict_data_files

        with pytest.raises(DatasetError, match=r"top-level argument"):
            kedro_dataset_cls(**args)

    def test_save_and_load_iterable_dataset(
        self, iterable_dataset, kedro_dataset_cls, path_file
    ):
        kedro_dataset = kedro_dataset_cls(path=path_file)
        with pytest.raises(DatasetError, match=r"got iterable dataset"):
            kedro_dataset.save(iterable_dataset)

    def test_save_and_load_iterable_dataset_dict(
        self,
        iterable_dataset_dict,
        kedro_dataset_cls,
        path_dir,
        dataset_dict_data_files,
    ):
        kedro_dataset = kedro_dataset_cls(
            path=path_dir, data_files=dataset_dict_data_files
        )
        with pytest.raises(DatasetError, match=r"got iterable dataset"):
            kedro_dataset.save(iterable_dataset_dict)

    def test_exists(self, hf_dataset, kedro_dataset_cls, path_file):
        kedro_dataset = kedro_dataset_cls(path=path_file)
        kedro_dataset.save(hf_dataset)
        assert kedro_dataset.exists()

    def test_load_missing_dataset(self, kedro_dataset_cls, path_file):
        kedro_dataset = kedro_dataset_cls(path=path_file)

        pattern = rf"Failed while loading data from dataset {_qualname(kedro_dataset_cls)}\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            kedro_dataset.load()

    def test_save_invalid_type(self, kedro_dataset_cls, path_file):
        kedro_dataset = kedro_dataset_cls(path=path_file)

        pattern = rf"{kedro_dataset_cls.__name__} only supports"
        with pytest.raises(DatasetError, match=pattern):
            kedro_dataset.save({"not": "a dataset"})

    @pytest.mark.parametrize("base_path,instance_type", PROTOCOLS)
    def test_protocol_usage(
        self, kedro_dataset_cls, extension, base_path, instance_type, mocker
    ):
        # Skip checking directory as it would require permissions for remote filesystems.
        mocker.patch.object(instance_type, "isdir", return_value=False)

        path = f"{base_path}{extension}"
        ds = kedro_dataset_cls(path=path)
        assert isinstance(ds._fs, instance_type)
        resolved = path.split(PROTOCOL_DELIMITER, 1)[-1]
        assert str(ds._filepath) == resolved
        assert isinstance(ds._filepath, PurePosixPath)

    def test_pathlike_path(self, hf_dataset, kedro_dataset_cls, tmp_path, extension):
        path = tmp_path / f"test_pathlike{extension}"
        ds = kedro_dataset_cls(path=path)
        ds.save(hf_dataset)
        reloaded = ds.load()
        assert isinstance(reloaded, DatasetDict)
        assert reloaded["train"].to_dict() == hf_dataset.to_dict()

    def test_catalog_release(self, kedro_dataset_cls, path_file, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        fs_mock.isdir.return_value = False
        ds = kedro_dataset_cls(path=path_file)
        ds.release()
        fs_mock.invalidate_cache.assert_called_once_with(path_file)


class TestFilesystemDatasetVersioned:
    def test_version_str_repr(
        self, kedro_dataset_cls, extension, load_version, save_version
    ):
        path = f"test{extension}"
        ds = kedro_dataset_cls(path=path)
        ds_versioned = kedro_dataset_cls(
            path=path, version=Version(load_version, save_version)
        )
        assert path in str(ds)
        assert "version" not in str(ds)

        assert path in str(ds_versioned)
        ver_str = f"version=Version(load='{load_version}', save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert kedro_dataset_cls.__name__ in str(ds_versioned)

    def test_save_and_load(self, hf_dataset, versioned_fs_dataset):
        versioned_fs_dataset.save(hf_dataset)
        reloaded = versioned_fs_dataset.load()
        assert isinstance(reloaded, DatasetDict)
        assert reloaded["train"].to_dict() == hf_dataset.to_dict()

    def test_no_versions(self, kedro_dataset_cls, path_file):
        """With Version(None, None), loading fails when no saved versions exist."""
        pattern = rf"Did not find any versions for {_qualname(kedro_dataset_cls)}\(.+\)"
        ds = kedro_dataset_cls(path=path_file, version=Version(None, None))
        with pytest.raises(DatasetError, match=pattern):
            ds.load()

    def test_exists(self, hf_dataset, versioned_fs_dataset):
        assert not versioned_fs_dataset.exists()
        versioned_fs_dataset.save(hf_dataset)
        assert versioned_fs_dataset.exists()

    def test_prevent_overwrite(
        self, hf_dataset, versioned_fs_dataset, kedro_dataset_cls
    ):
        versioned_fs_dataset.save(hf_dataset)
        pattern = (
            rf"Save path \'.+\' for {_qualname(kedro_dataset_cls)}\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_fs_dataset.save(hf_dataset)

    def test_exists_no_versions(self, kedro_dataset_cls, path_file):
        """`exists()` returns False (not raises) when no versions are saved yet."""
        ds = kedro_dataset_cls(path=path_file, version=Version(None, None))
        assert ds.exists() is False
