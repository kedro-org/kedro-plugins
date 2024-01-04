from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from pandas.testing import assert_frame_equal
from s3fs.core import S3FileSystem

from kedro_datasets.yaml import YAMLDataset


@pytest.fixture
def filepath_yaml(tmp_path):
    return (tmp_path / "test.yaml").as_posix()


@pytest.fixture
def yaml_dataset(filepath_yaml, save_args, fs_args):
    return YAMLDataset(filepath=filepath_yaml, save_args=save_args, fs_args=fs_args)


@pytest.fixture
def versioned_yaml_dataset(filepath_yaml, load_version, save_version):
    return YAMLDataset(
        filepath=filepath_yaml, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_data():
    return {"col1": 1, "col2": 2, "col3": 3}


class TestYAMLDataset:
    def test_save_and_load(self, yaml_dataset, dummy_data):
        """Test saving and reloading the data set."""
        yaml_dataset.save(dummy_data)
        reloaded = yaml_dataset.load()
        assert dummy_data == reloaded
        assert yaml_dataset._fs_open_args_load == {}
        assert yaml_dataset._fs_open_args_save == {"mode": "w"}

    def test_exists(self, yaml_dataset, dummy_data):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not yaml_dataset.exists()
        yaml_dataset.save(dummy_data)
        assert yaml_dataset.exists()

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, yaml_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert yaml_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, yaml_dataset, fs_args):
        assert yaml_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert yaml_dataset._fs_open_args_save == {"mode": "w"}  # default unchanged

    def test_load_missing_file(self, yaml_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set YAMLDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            yaml_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.yaml", S3FileSystem),
            ("file:///tmp/test.yaml", LocalFileSystem),
            ("/tmp/test.yaml", LocalFileSystem),
            ("gcs://bucket/file.yaml", GCSFileSystem),
            ("https://example.com/file.yaml", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = YAMLDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.yaml"
        dataset = YAMLDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_dataframe_support(self, yaml_dataset):
        data = pd.DataFrame({"col1": [1, 2], "col2": [4, 5]})
        yaml_dataset.save(data.to_dict())
        reloaded = yaml_dataset.load()
        assert isinstance(reloaded, dict)

        data_df = pd.DataFrame.from_dict(reloaded)
        assert_frame_equal(data, data_df)


class TestYAMLDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.yaml"
        ds = YAMLDataset(filepath=filepath)
        ds_versioned = YAMLDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "YAMLDataset" in str(ds_versioned)
        assert "YAMLDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        # Default save_args
        assert "save_args={'default_flow_style': False}" in str(ds)
        assert "save_args={'default_flow_style': False}" in str(ds_versioned)

    def test_save_and_load(self, versioned_yaml_dataset, dummy_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_yaml_dataset.save(dummy_data)
        reloaded = versioned_yaml_dataset.load()
        assert dummy_data == reloaded

    def test_no_versions(self, versioned_yaml_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for YAMLDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_yaml_dataset.load()

    def test_exists(self, versioned_yaml_dataset, dummy_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_yaml_dataset.exists()
        versioned_yaml_dataset.save(dummy_data)
        assert versioned_yaml_dataset.exists()

    def test_prevent_overwrite(self, versioned_yaml_dataset, dummy_data):
        """Check the error when attempting to override the data set if the
        corresponding yaml file for a given save version already exists."""
        versioned_yaml_dataset.save(dummy_data)
        pattern = (
            r"Save path \'.+\' for YAMLDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_yaml_dataset.save(dummy_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_yaml_dataset, load_version, save_version, dummy_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for YAMLDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_yaml_dataset.save(dummy_data)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            YAMLDataset(
                filepath="https://example.com/file.yaml", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, yaml_dataset, versioned_yaml_dataset, dummy_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        yaml_dataset.save(dummy_data)
        assert yaml_dataset.exists()
        assert yaml_dataset._filepath == versioned_yaml_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_yaml_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_yaml_dataset.save(dummy_data)

        # Remove non-versioned dataset and try again
        Path(yaml_dataset._filepath.as_posix()).unlink()
        versioned_yaml_dataset.save(dummy_data)
        assert versioned_yaml_dataset.exists()
