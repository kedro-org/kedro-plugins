from pathlib import Path, PurePosixPath

import numpy as np
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io import DatasetError
from kedro.io.core import PROTOCOL_DELIMITER, Version
from s3fs.core import S3FileSystem

from kedro_datasets.matlab import MatlabDataset


@pytest.fixture
def filepath_matlab(tmp_path):
    return (tmp_path / "test.mat").as_posix()


@pytest.fixture
def matlab_dataset(filepath_matlab, save_args, fs_args):
    return MatlabDataset(filepath=filepath_matlab, save_args=save_args, fs_args=fs_args)


@pytest.fixture
def versioned_matlab_dataset(filepath_matlab, load_version, save_version):
    return MatlabDataset(
        filepath=filepath_matlab, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_data():
    return np.array([1, 2, 3, 4, 5])


class TestMatlabDataset:
    def test_save_and_load(self, matlab_dataset, dummy_data):
        """Test saving and reloading the data set."""
        matlab_dataset.save(dummy_data)
        reloaded = matlab_dataset.load()
        assert (dummy_data == reloaded["data"]).all()
        assert matlab_dataset._fs_open_args_load == {}
        assert matlab_dataset._fs_open_args_save == {"mode": "w"}

    def test_exists(self, matlab_dataset, dummy_data):
        """Test `exists` method invocation for both existing and
        nonexistent data set."""
        assert not matlab_dataset.exists()
        matlab_dataset.save(dummy_data)
        assert matlab_dataset.exists()

    @pytest.mark.parametrize(
        "save_args", [{"k1": "v1", "index": "value"}], indirect=True
    )
    def test_save_extra_params(self, matlab_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert matlab_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, matlab_dataset, fs_args):
        assert matlab_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert matlab_dataset._fs_open_args_save == {"mode": "w"}  # default unchanged

    def test_load_missing_file(self, matlab_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from data set MatlabDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            matlab_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.mat", S3FileSystem),
            ("file:///tmp/test.mat", LocalFileSystem),
            ("/tmp/test.mat", LocalFileSystem),
            ("gcs://bucket/file.mat", GCSFileSystem),
            ("https://example.com/file.mat", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = MatlabDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.json"
        dataset = MatlabDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestMatlabDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.mat"
        ds = MatlabDataset(filepath=filepath)
        ds_versioned = MatlabDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "MatlabDataset" in str(ds_versioned)
        assert "MatlabDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        # Default save_args
        assert "save_args={'indent': 2}" in str(ds)
        assert "save_args={'indent': 2}" in str(ds_versioned)

    def test_save_and_load(self, versioned_matlab_dataset, dummy_data):
        """Test that saved and reloaded data matches the original one for
        the versioned data set."""
        versioned_matlab_dataset.save(dummy_data)
        reloaded = versioned_matlab_dataset.load()
        assert (dummy_data == reloaded["data"]).all()

    def test_no_versions(self, versioned_matlab_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for MatlabDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_matlab_dataset.load()

    def test_exists(self, versioned_matlab_dataset, dummy_data):
        """Test `exists` method invocation for versioned data set."""
        assert not versioned_matlab_dataset.exists()
        versioned_matlab_dataset.save(dummy_data)
        assert versioned_matlab_dataset.exists()

    def test_prevent_overwrite(self, versioned_matlab_dataset, dummy_data):
        """Check the error when attempting to override the data set if the
        corresponding json file for a given save version already exists."""
        versioned_matlab_dataset.save(dummy_data)
        pattern = (
            r"Save path \'.+\' for MatlabDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_matlab_dataset.save(dummy_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_matlab_dataset, load_version, save_version, dummy_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            f"Save version '{save_version}' did not match "
            f"load version '{load_version}' for "
            r"MatlabDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_matlab_dataset.save(dummy_data)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            MatlabDataset(
                filepath="https://example.com/file.mat", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, matlab_dataset, versioned_matlab_dataset, dummy_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        matlab_dataset.save(dummy_data)
        assert matlab_dataset.exists()
        assert matlab_dataset._filepath == versioned_matlab_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_matlab_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_matlab_dataset.save(dummy_data)

        # Remove non-versioned dataset and try again
        Path(matlab_dataset._filepath.as_posix()).unlink()
        versioned_matlab_dataset.save(dummy_data)
        assert versioned_matlab_dataset.exists()
