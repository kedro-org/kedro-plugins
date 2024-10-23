import tempfile
from pathlib import Path, PurePosixPath

import pytest
import torch
from fsspec.implementations.http import HTTPFileSystem
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from kedro.io.core import PROTOCOL_DELIMITER, DatasetError, Version
from s3fs.core import S3FileSystem

from kedro_datasets_experimental.safetensors import SafetensorsDataset


@pytest.fixture
def filepath(tmp_path):
    return (tmp_path / "test.safetensors").as_posix()


@pytest.fixture(params=["torch"])
def backend(request):
    return request.param


@pytest.fixture
def safetensors_dataset(filepath, backend, fs_args):
    return SafetensorsDataset(
        filepath=filepath,
        backend=backend,
        fs_args=fs_args,
    )


@pytest.fixture
def versioned_safetensors_dataset(filepath, load_version, save_version):
    return SafetensorsDataset(
        filepath=filepath, version=Version(load_version, save_version)
    )


@pytest.fixture
def dummy_data():
    return {"embeddings": torch.zeros((10, 100))}


class TestSafetensorsDataset:
    @pytest.mark.parametrize(
        "backend",
        [
            "torch",
        ],
        indirect=True,
    )
    def test_save_and_load(self, safetensors_dataset, dummy_data):
        """Test saving and reloading the dataset."""
        safetensors_dataset.save(dummy_data)
        reloaded = safetensors_dataset.load()

        if safetensors_dataset._backend == "torch":
            assert torch.equal(dummy_data["embeddings"], reloaded["embeddings"])

        assert safetensors_dataset._fs_open_args_load == {}
        assert safetensors_dataset._fs_open_args_save == {"mode": "wb"}

    def test_exists(self, safetensors_dataset, dummy_data):
        """Test `exists` method invocation for both existing and
        nonexistent dataset."""
        assert not safetensors_dataset.exists()
        safetensors_dataset.save(dummy_data)
        assert safetensors_dataset.exists()

    @pytest.mark.parametrize(
        "fs_args",
        [{"open_args_load": {"mode": "rb", "compression": "gzip"}}],
        indirect=True,
    )
    def test_open_extra_args(self, safetensors_dataset, fs_args):
        assert safetensors_dataset._fs_open_args_load == fs_args["open_args_load"]
        assert safetensors_dataset._fs_open_args_save == {"mode": "wb"}  # default unchanged

    def test_load_missing_file(self, safetensors_dataset):
        """Check the error when trying to load missing file."""
        pattern = r"Failed while loading data from dataset SafetensorsDataset\(.*\)"
        with pytest.raises(DatasetError, match=pattern):
            safetensors_dataset.load()

    @pytest.mark.parametrize(
        "filepath,instance_type",
        [
            ("s3://bucket/file.safetensors", S3FileSystem),
            (tempfile.NamedTemporaryFile(suffix=".safetensors").name, LocalFileSystem),
            (tempfile.NamedTemporaryFile(suffix=".safetensors").name, LocalFileSystem),
            ("gcs://bucket/file.safetensors", GCSFileSystem),
            ("https://example.com/file.safetensors", HTTPFileSystem),
        ],
    )
    def test_protocol_usage(self, filepath, instance_type):
        dataset = SafetensorsDataset(filepath=filepath)
        assert isinstance(dataset._fs, instance_type)

        path = filepath.split(PROTOCOL_DELIMITER, 1)[-1]

        assert str(dataset._filepath) == path
        assert isinstance(dataset._filepath, PurePosixPath)

    def test_catalog_release(self, mocker):
        fs_mock = mocker.patch("fsspec.filesystem").return_value
        filepath = "test.safetensors"
        dataset = SafetensorsDataset(filepath=filepath)
        dataset.release()
        fs_mock.invalidate_cache.assert_called_once_with(filepath)

    def test_invalid_backend(self, mocker):
        pattern = (
            r"Selected backend 'invalid' could not be imported. "
            r"Make sure it is installed and importable."
        )
        mocker.patch(
            "kedro_datasets_experimental.safetensors.safetensors_dataset.importlib.import_module",
            side_effect=ImportError,
        )
        with pytest.raises(ImportError, match=pattern):
            SafetensorsDataset(filepath="test.safetensors", backend="invalid")

    def test_copy(self, safetensors_dataset):
        safetensors_dataset_copy = safetensors_dataset._copy()
        assert safetensors_dataset_copy is not safetensors_dataset
        assert safetensors_dataset_copy._describe() == safetensors_dataset._describe()


class TestSafetensorsDatasetVersioned:
    def test_version_str_repr(self, load_version, save_version):
        """Test that version is in string representation of the class instance
        when applicable."""
        filepath = "test.safetensors"
        ds = SafetensorsDataset(filepath=filepath)
        ds_versioned = SafetensorsDataset(
            filepath=filepath, version=Version(load_version, save_version)
        )
        assert filepath in str(ds)
        assert "version" not in str(ds)

        assert filepath in str(ds_versioned)
        ver_str = f"version=Version(load={load_version}, save='{save_version}')"
        assert ver_str in str(ds_versioned)
        assert "SafetensorsDataset" in str(ds_versioned)
        assert "SafetensorsDataset" in str(ds)
        assert "protocol" in str(ds_versioned)
        assert "protocol" in str(ds)
        assert "backend" in str(ds_versioned)
        assert "backend" in str(ds)

    def test_save_and_load(self, versioned_safetensors_dataset, dummy_data):
        """Test that saved and reloaded data matches the original one for
        the versioned dataset."""
        versioned_safetensors_dataset.save(dummy_data)
        reloaded_df = versioned_safetensors_dataset.load()

        assert torch.equal(dummy_data["embeddings"], reloaded_df["embeddings"])

    def test_no_versions(self, versioned_safetensors_dataset):
        """Check the error if no versions are available for load."""
        pattern = r"Did not find any versions for SafetensorsDataset\(.+\)"
        with pytest.raises(DatasetError, match=pattern):
            versioned_safetensors_dataset.load()

    def test_exists(self, versioned_safetensors_dataset, dummy_data):
        """Test `exists` method invocation for versioned dataset."""
        assert not versioned_safetensors_dataset.exists()
        versioned_safetensors_dataset.save(dummy_data)
        assert versioned_safetensors_dataset.exists()

    def test_prevent_overwrite(self, versioned_safetensors_dataset, dummy_data):
        """Check the error when attempting to override the dataset if the
        corresponding Safetensors file for a given save version already exists."""
        versioned_safetensors_dataset.save(dummy_data)
        pattern = (
            r"Save path \'.+\' for SafetensorsDataset\(.+\) must "
            r"not exist if versioning is enabled\."
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_safetensors_dataset.save(dummy_data)

    @pytest.mark.parametrize(
        "load_version", ["2019-01-01T23.59.59.999Z"], indirect=True
    )
    @pytest.mark.parametrize(
        "save_version", ["2019-01-02T00.00.00.000Z"], indirect=True
    )
    def test_save_version_warning(
        self, versioned_safetensors_dataset, load_version, save_version, dummy_data
    ):
        """Check the warning when saving to the path that differs from
        the subsequent load path."""
        pattern = (
            rf"Save version '{save_version}' did not match load version "
            rf"'{load_version}' for SafetensorsDataset\(.+\)"
        )
        with pytest.warns(UserWarning, match=pattern):
            versioned_safetensors_dataset.save(dummy_data)

    def test_http_filesystem_no_versioning(self):
        pattern = "Versioning is not supported for HTTP protocols."

        with pytest.raises(DatasetError, match=pattern):
            SafetensorsDataset(
                filepath="https://example.com/file.safetensors", version=Version(None, None)
            )

    def test_versioning_existing_dataset(
        self, safetensors_dataset, versioned_safetensors_dataset, dummy_data
    ):
        """Check the error when attempting to save a versioned dataset on top of an
        already existing (non-versioned) dataset."""
        safetensors_dataset.save(dummy_data)
        assert safetensors_dataset.exists()
        assert safetensors_dataset._filepath == versioned_safetensors_dataset._filepath
        pattern = (
            f"(?=.*file with the same name already exists in the directory)"
            f"(?=.*{versioned_safetensors_dataset._filepath.parent.as_posix()})"
        )
        with pytest.raises(DatasetError, match=pattern):
            versioned_safetensors_dataset.save(dummy_data)

        # Remove non-versioned dataset and try again
        Path(safetensors_dataset._filepath.as_posix()).unlink()
        versioned_safetensors_dataset.save(dummy_data)
        assert versioned_safetensors_dataset.exists()

    def test_copy(self, versioned_safetensors_dataset):
        safetensors_dataset_copy = versioned_safetensors_dataset._copy()
        assert safetensors_dataset_copy is not versioned_safetensors_dataset
        assert safetensors_dataset_copy._describe() == versioned_safetensors_dataset._describe()
