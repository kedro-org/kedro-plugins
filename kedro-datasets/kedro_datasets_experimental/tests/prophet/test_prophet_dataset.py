from pathlib import Path, PurePosixPath

import pandas as pd
import pytest
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from prophet import Prophet
from s3fs.core import S3FileSystem

from kedro_datasets_experimental.prophet import ProphetModelDataset


@pytest.fixture
def filepath_json(tmp_path):
    return (tmp_path / "test_model.json").as_posix()


@pytest.fixture
def prophet_model_dataset(filepath_json, save_args, fs_args):
    return ProphetModelDataset(
        filepath=filepath_json, save_args=save_args, fs_args=fs_args
    )


@pytest.fixture
def versioned_prophet_model_dataset(filepath_json, load_version, save_version):
    return ProphetModelDataset(
        filepath=filepath_json, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_model():
    df = pd.DataFrame({"ds": ["2024-01-01", "2024-01-02", "2024-01-03"], "y": [100, 200, 300]})
    model = Prophet()
    # Fit the model with dummy data
    model.fit(df)
    return model


class TestProphetModelDataset:
    def test_save_and_load(self, prophet_model_dataset, dummy_model):
        """Test saving and reloading the Prophet model."""
        prophet_model_dataset.save(dummy_model)
        reloaded = prophet_model_dataset.load()
        assert isinstance(reloaded, Prophet)
        assert prophet_model_dataset._fs_open_args_load == {}
        assert prophet_model_dataset._fs_open_args_save == {"mode": "w"}

    def test_exists(self, prophet_model_dataset, dummy_model):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not prophet_model_dataset.exists()
        prophet_model_dataset.save(dummy_model)
        assert prophet_model_dataset.exists()

    @pytest.mark.parametrize("save_args", [{"k1": "v1", "indent": 4}], indirect=True)
    def test_save_extra_params(self, prophet_model_dataset, save_args):
        """Test overriding the default save arguments."""
        for key, value in save_args.items():
            assert prophet_model_dataset._save_args[key] == value

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, prophet_model_dataset, fs_args):
        assert prophet_model_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert prophet_model_dataset._fs_open_args_save == {
            "mode": "w"
        }  # default unchanged

    def test_load_missing_file(self, prophet_model_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset ProphetModelDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            prophet_model_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/model.json", S3FileSystem),
            ("file:///tmp/test_model.json", LocalFileSystem),
            ("/tmp/test_model.json", LocalFileSystem),  #nosec: B108
            ("gcs://bucket/model.json", GCSFileSystem),
            ("https://example.com/model.json", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = ProphetModelDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test_model.json"
        dataset = ProphetModelDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)


class TestProphetModelDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test_model.json"
        ds = ProphetModelDataset(filepath=filepath)
        ds_versioned = ProphetModelDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "ProphetModelDataset" in str(ds_versioned)
        assert "ProphetModelDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        # Default save_args
        assert "save_args={'indent': 2}" in str(ds)
        assert "save_args={'indent': 2}" in str(ds_versioned)

    def test_save_and_load(self, versioned_prophet_model_dataset, dummy_model):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_prophet_model_dataset.save(dummy_model)
        reloaded = versioned_prophet_model_dataset.load()
        assert isinstance(reloaded, Prophet)

    def test_no_versions(self, versioned_prophet_model_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for ProphetModelDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_prophet_model_dataset.load()

    def test_exists(self, versioned_prophet_model_dataset, dummy_model):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_prophet_model_dataset.exists()
        versioned_prophet_model_dataset.save(dummy_model)
        assert versioned_prophet_model_dataset.exists()

    def test_prevent_overwrite(self, versioned_prophet_model_dataset, dummy_model):
        """Check the error when attempting to override the dataset if the
        corresponding json file for a given save version already exists."""
        versioned_prophet_model_dataset.save(dummy_model)
        pattern = (
            r"Save path \'.+\' for ProphetModelDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_prophet_model_dataset.save(dummy_model)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_prophet_model_dataset, load_version, save_version, dummy_model
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            f"Save version '{save_version}' did not match "
            f"load version '{load_version}' for "
            r"ProphetModelDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_prophet_model_dataset.save(dummy_model)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            ProphetModelDataset(
                filepath="https://example.com/model.json", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, prophet_model_dataset, versioned_prophet_model_dataset, dummy_model
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        prophet_model_dataset.save(dummy_model)
        assert prophet_model_dataset.exists()
        assert (
            prophet_model_dataset._filepath == versioned_prophet_model_dataset._filepath
        )
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_prophet_model_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_prophet_model_dataset.save(dummy_model)

        # Remove non-versioned dataset and try again
        Path(prophet_model_dataset._filepath.as_posix()).unlink()
        versioned_prophet_model_dataset.save(dummy_model)
        assert versioned_prophet_model_dataset.exists()
