import inspect
import json
from pathlib import Path, PurePosixPath

import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets.json import JSONDataset


@pytest.fixture
def filepath_json(tmp_path):
    return (tmp_path / "test.json").as_posix()


@pytest.fixture
def json_dataset(filepath_json, save_args, fs_args):
    return JSONDataset(filepath=filepath_json, save_args=save_args, fs_args=fs_args)


@pytest.fixture
def versioned_json_dataset(filepath_json, load_version, save_version):
    return JSONDataset(
        filepath=filepath_json, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_data():
    return {"col1": 1, "col2": 2, "col3": 3}


class TestJSONDataset:
    def test_save_and_load(self, json_dataset, dummy_data):
        """Test saving and reloading the data set."""
        json_dataset.save(dummy_data)
        reloaded = json_dataset.load()
        assert dummy_data == reloaded
        assert json_dataset._fs_open_args_load == {}
        assert json_dataset._fs_open_args_save == {"mode": "w"}

    def test_exists(self, json_dataset, dummy_data):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not json_dataset.exists()
        json_dataset.save(dummy_data)
        assert json_dataset.exists()

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, json_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert json_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, json_dataset, fs_args):
        assert json_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert json_dataset._fs_open_args_save == {"mode": "w"}  # default unchanged

    def test_load_missing_file(self, json_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set JSONDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            json_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.json", S3FileSystem),
            ("file:///tmp/test.json", LocalFileSystem),
            ("/tmp/test.json", LocalFileSystem),
            ("gcs://bucket/file.json", GCSFileSystem),
            ("https://example.com/file.json", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = JSONDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.json"
        dataset = JSONDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestJSONDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.json"
        ds = JSONDataset(filepath=filepath)
        ds_versioned = JSONDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "JSONDataset" in str(ds_versioned)
        assert "JSONDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        # Default save_args
        assert "save_args={'indent': 2}" in str(ds)
        assert "save_args={'indent': 2}" in str(ds_versioned)

    def test_save_and_load(self, versioned_json_dataset, dummy_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_json_dataset.save(dummy_data)
        reloaded = versioned_json_dataset.load()
        assert dummy_data == reloaded

    def test_no_versions(self, versioned_json_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for JSONDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.load()

    def test_exists(self, versioned_json_dataset, dummy_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_json_dataset.exists()
        versioned_json_dataset.save(dummy_data)
        assert versioned_json_dataset.exists()

    def test_prevent_overwrite(self, versioned_json_dataset, dummy_data):
        """Check the error when attempting to override the data set if the
        corresponding json file for a given save version already exists."""
        versioned_json_dataset.save(dummy_data)
        pattern = (
            r"Save path \'.+\' for JSONDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.save(dummy_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_json_dataset, load_version, save_version, dummy_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            f"Save version '{save_version}' did not match "
            f"load version '{load_version}' for "
            r"JSONDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_json_dataset.save(dummy_data)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            JSONDataset(
                filepath="https://example.com/file.json", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, json_dataset, versioned_json_dataset, dummy_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        json_dataset.save(dummy_data)
        assert json_dataset.exists()
        assert json_dataset._filepath == versioned_json_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_json_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_json_dataset.save(dummy_data)

        # Remove non-versioned dataset and try again
        Path(json_dataset._filepath.as_posix()).unlink()
        versioned_json_dataset.save(dummy_data)
        assert versioned_json_dataset.exists()

    def test_preview(self, json_dataset, dummy_data):
        """Test the preview method."""
        json_dataset.save(dummy_data)
        preview_data = json_dataset.preview()

        # Load the data directly for comparison
        with json_dataset._fs.open(json_dataset._get_load_path(), mode="r") as fs_file:
            full_data = json.load(fs_file)

        expected_data = json.dumps(full_data)

        assert (
            preview_data == expected_data
        ), "The preview data does not match the expected data."
        assert (
            inspect.signature(json_dataset.preview).return_annotation == "JSONPreview"
        )
